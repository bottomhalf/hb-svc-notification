package com.bot.notificationservice.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaNotificationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNotificationService.class);

    @Autowired
    private PushNotificationService pushNotificationService;

    @KafkaListener(
            topics = "${kafka.topic.hb-post-event:${kafka.topic:hb-post-create-update-event}}",
            groupId = "${spring.kafka.consumer.group-id:${kafka.group_id:bh-notification-group}}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePostEvent(String message) {
        LOGGER.info("[KAFKA NOTIFICATION SERVICE]: Consumed message from topic: {}", message);
        try {
            String result = pushNotificationService.processNotification(message);
            LOGGER.info("[KAFKA NOTIFICATION SERVICE]: Notification processed and sent successfully. Result: {}", result);
        } catch (Exception ex) {
            LOGGER.error("[KAFKA NOTIFICATION SERVICE]: Error while processing consumed notification: {}", ex.getMessage(), ex);
        }
    }
}
