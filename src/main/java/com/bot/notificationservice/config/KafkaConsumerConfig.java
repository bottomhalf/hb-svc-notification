package com.bot.notificationservice.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092,localhost:9093,localhost:9094}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:${kafka.group_id:bh-notification-group}}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${kafka.max_retries:3}")
    private int maxRetries;

    @Value("${spring.kafka.listener.auto-startup:true}")
    private boolean autoStartup;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, maxRetries)));
        factory.getContainerProperties().setMissingTopicsFatal(false);

        boolean resolvable = canResolveBootstrapServers(bootstrapServers);
        if (!resolvable) {
            LOGGER.warn("[KAFKA CONFIG]: Bootstrap servers '{}' are not resolvable on this host. Disabling Kafka listener auto-startup to avoid ApplicationContext startup failure.", bootstrapServers);
            factory.setAutoStartup(false);
        } else {
            factory.setAutoStartup(autoStartup);
        }

        return factory;
    }

    private boolean canResolveBootstrapServers(String servers) {
        if (servers == null || servers.isBlank()) {
            return false;
        }
        for (String server : servers.split(",")) {
            String host = server.trim();
            if (host.contains(":")) {
                host = host.substring(0, host.indexOf(":"));
            }
            try {
                InetAddress.getByName(host);
                return true;
            } catch (Exception ignored) {
            }
        }
        return false;
    }
}
