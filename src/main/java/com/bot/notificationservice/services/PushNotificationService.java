package com.bot.notificationservice.services;

import com.bot.notificationservice.model.NotificationMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fierhub.service.FierhubService;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.BatchResponse;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class PushNotificationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushNotificationService.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired(required = false)
    @Qualifier("notificationExecutor")
    private Executor notificationExecutor;

    @Autowired
    FierhubService fierhubService;

    @PostConstruct
    private void initializeFirebase() {
        try {
            if (FirebaseApp.getApps().isEmpty()) {
                Map<String, Object> serviceAccountMap = new HashMap<>();
                serviceAccountMap.put("type", getServiceAccountKey("type", "service_account"));
                serviceAccountMap.put("project_id", getServiceAccountKey("project_id", null));
                serviceAccountMap.put("private_key_id", getServiceAccountKey("private_key_id", null));
                serviceAccountMap.put("private_key", getServiceAccountKey("private_key", null));
                serviceAccountMap.put("client_email", getServiceAccountKey("client_email", null));
                serviceAccountMap.put("client_id", getServiceAccountKey("client_id", null));
                serviceAccountMap.put("auth_uri", getServiceAccountKey("auth_uri", "https://accounts.google.com/o/oauth2/auth"));
                serviceAccountMap.put("token_uri", getServiceAccountKey("token_uri", "https://oauth2.googleapis.com/token"));
                serviceAccountMap.put("auth_provider_x509_cert_url", getServiceAccountKey("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"));
                serviceAccountMap.put("client_x509_cert_url", getServiceAccountKey("client_x509_cert_url", null));
                serviceAccountMap.put("universe_domain", getServiceAccountKey("universe_domain", "googleapis.com"));

                InputStream serviceAccountStream;
                String privateKey = (String) serviceAccountMap.get("private_key");
                if (privateKey != null && !privateKey.trim().isEmpty()) {
                    if (privateKey.contains("\\n")) {
                        serviceAccountMap.put("private_key", privateKey.replace("\\n", "\n"));
                    }
                    byte[] jsonBytes = objectMapper.writeValueAsBytes(serviceAccountMap);
                    serviceAccountStream = new ByteArrayInputStream(jsonBytes);
                    LOGGER.info("Loaded Firebase credentials via FierhubService configuration.");
                } else {
                    LOGGER.warn("Firebase private_key not found in FierhubService configuration. Falling back to serviceAccountKey.json");
                    serviceAccountStream = PushNotificationService.class.getClassLoader()
                            .getResourceAsStream("serviceAccountKey.json");
                    if (serviceAccountStream == null) {
                        LOGGER.warn("serviceAccountKey.json not found in classpath. Firebase initialization skipped.");
                        return;
                    }
                }

                FirebaseOptions options = FirebaseOptions.builder()
                        .setCredentials(GoogleCredentials.fromStream(serviceAccountStream))
                        .build();
                FirebaseApp.initializeApp(options);
                LOGGER.info("Firebase has been initialized successfully.");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to initialize Firebase: {}", ex.getMessage());
        }
    }

    private String getServiceAccountKey(String key, String defaultValue) {
        String value = fierhubService.getConfiguration(key, String.class);
        if (value == null || value.trim().isEmpty()) {
            value = fierhubService.getConfiguration("firebase." + key, String.class);
        }
        if (value == null || value.trim().isEmpty()) {
            value = fierhubService.getConfiguration("firebase." + key.replace('_', '-'), String.class);
        }
        if (value == null || value.trim().isEmpty()) {
            value = defaultValue;
        }
        return value;
    }

    public String processNotification(String kafkaMessage) throws Exception {
        LOGGER.info("Processing consumed Kafka message: {}", kafkaMessage);
        if (kafkaMessage == null || kafkaMessage.isBlank()) {
            LOGGER.warn("Received empty or null Kafka message, skipping notification.");
            return "skipped_empty_message";
        }

        NotificationMessage model;
        try {
            if (kafkaMessage.trim().startsWith("[")) {
                List<NotificationMessage> list = objectMapper.readValue(
                        kafkaMessage,
                        new TypeReference<List<NotificationMessage>>() {
                        }
                );
                return sendPushNotificationBatch(list);
            } else if (kafkaMessage.trim().startsWith("{")) {
                model = objectMapper.readValue(kafkaMessage, NotificationMessage.class);
            } else {
                model = new NotificationMessage();
                model.setTitle("New Job Alert");
                model.setDescription(kafkaMessage);
                model.setTopic("all");
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to parse message as JSON: {}. Using fallback model.", e.getMessage());
            model = new NotificationMessage();
            model.setTitle("New Job Alert");
            model.setDescription(kafkaMessage);
            model.setTopic("all");
        }

        return sendPushNotification(model);
    }

    public String sendPushNotificationBatch(List<NotificationMessage> list) {
        if (list == null || list.isEmpty()) {
            return "empty_batch";
        }

        LOGGER.info("Processing batch of {} notifications in parallel", list.size());
        Executor executor = notificationExecutor != null ? notificationExecutor : ForkJoinPool.commonPool();

        // Firebase sendEach accepts up to 500 messages per call
        int batchSize = 500;
        List<List<NotificationMessage>> subBatches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            subBatches.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        List<CompletableFuture<Void>> futures = subBatches.stream()
                .map(subBatch -> CompletableFuture.runAsync(() -> {
                    List<Message> firebaseMessages = new ArrayList<>();
                    for (NotificationMessage item : subBatch) {
                        try {
                            Message msg = createFirebaseMessage(item);
                            if (msg != null) {
                                firebaseMessages.add(msg);
                            }
                        } catch (Exception ex) {
                            failureCount.incrementAndGet();
                            LOGGER.error("Failed to build Firebase message for follower {}: {}", item.getFollowerId(), ex.getMessage());
                        }
                    }

                    if (!firebaseMessages.isEmpty()) {
                        try {
                            BatchResponse response = FirebaseMessaging.getInstance().sendEach(firebaseMessages);
                            successCount.addAndGet(response.getSuccessCount());
                            failureCount.addAndGet(response.getFailureCount());
                            if (response.getFailureCount() > 0) {
                                LOGGER.warn("Firebase batch finished with {} successes and {} failures out of {} messages",
                                        response.getSuccessCount(), response.getFailureCount(), firebaseMessages.size());
                            }
                        } catch (Exception ex) {
                            failureCount.addAndGet(firebaseMessages.size());
                            LOGGER.error("Failed to send Firebase batch of {} messages: {}", firebaseMessages.size(), ex.getMessage(), ex);
                        }
                    }
                }, executor))
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        LOGGER.info("Parallel notification batch completed: {}/{} sent successfully ({} failed)",
                successCount.get(), list.size(), failureCount.get());

        return String.format("Processed batch of %d notifications: %d succeeded, %d failed",
                list.size(), successCount.get(), failureCount.get());
    }

    public Message createFirebaseMessage(NotificationMessage notificationMessage) {
        if (notificationMessage == null) {
            return null;
        }

        String title = notificationMessage.getTitle();
        if (title == null || title.isBlank()) {
            title = "New Job Alert";
        }

        String body = resolveBody(notificationMessage);
        if (body == null || body.isBlank()) {
            body = "A new job post update has been published.";
        }

        Notification.Builder notificationBuilder = Notification.builder()
                .setTitle(title)
                .setBody(body);

        if (notificationMessage.getImageUrl() != null && !notificationMessage.getImageUrl().isBlank()) {
            notificationBuilder.setImage("https://api.hiringbell.com/" + notificationMessage.getImageUrl());
        } else {
            notificationBuilder.setImage("https://api.hiringbell.com/hiringbell_logo.png");
        }

        Notification notification = notificationBuilder.build();

        long targetPostId = notificationMessage.getJobPostId() > 0 ? notificationMessage.getJobPostId() : notificationMessage.getPostId();
        String notificationType = notificationMessage.getNotificationType() != null && !notificationMessage.getNotificationType().isBlank()
                ? notificationMessage.getNotificationType()
                : "JOB_POST";

        Message.Builder messageBuilder = Message.builder()
                .setNotification(notification)
                .putData("notificationType", notificationType)
                .putData("followedId", String.valueOf(notificationMessage.getFollowedId()))
                .putData("jobPostId", String.valueOf(targetPostId))
                .putData("followerId", String.valueOf(notificationMessage.getFollowerId()))
                .putData("fullName", notificationMessage.getFullName() != null ? notificationMessage.getFullName() : "")
                .putData("description", body)
                .putData("imageUrl", notificationMessage.getImageUrl() != null ? notificationMessage.getImageUrl() : "")
                .putData("time", String.valueOf(System.currentTimeMillis()));

        if (notificationMessage.getData() != null && !notificationMessage.getData().isEmpty()) {
            notificationMessage.getData().forEach((k, v) -> {
                if (k != null && v != null) {
                    messageBuilder.putData(k, v);
                }
            });
        }

        String deviceToken = resolveDeviceToken(notificationMessage);
        if (deviceToken != null && !deviceToken.isBlank()) {
            messageBuilder.setToken(deviceToken);
            LOGGER.info("Targeting push notification to device token: {}", deviceToken);
        } else if (notificationMessage.getTopic() != null && !notificationMessage.getTopic().isBlank()) {
            messageBuilder.setTopic(notificationMessage.getTopic());
            LOGGER.info("Targeting push notification to topic: {}", notificationMessage.getTopic());
        } else {
            messageBuilder.setTopic("all");
            LOGGER.info("No specific device token found. Targeting push notification to topic 'all'");
        }

        return messageBuilder.build();
    }

    public String sendPushNotification(NotificationMessage notificationMessage) throws Exception {
        if (notificationMessage == null) {
            throw new IllegalArgumentException("Notification model cannot be null");
        }

        Message message = createFirebaseMessage(notificationMessage);
        String response = FirebaseMessaging.getInstance().send(message);
        LOGGER.info("Successfully sent push notification via Firebase: {}", response);
        return response;
    }

    private String resolveDeviceToken(NotificationMessage model) {
        if (model.getDeviceId() != null && !model.getDeviceId().isBlank()) {
            return model.getDeviceId();
        }
        if (model.getToken() != null && !model.getToken().isBlank()) {
            return model.getToken();
        }
        return null;
    }

    private String resolveBody(NotificationMessage model) {
        if (model.getDescription() != null && !model.getDescription().isBlank()) {
            return model.getDescription();
        }
        if (model.getBody() != null && !model.getBody().isBlank()) {
            return model.getBody();
        }
        if (model.getMessage() != null && !model.getMessage().isBlank()) {
            return model.getMessage();
        }
        return null;
    }
}
