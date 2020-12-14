package com.databoy.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * 〈一句话功能简述）
 * 〈〉
 *
 * @author by_zft_xiaopeng
 * @create 2020/12/14
 * @since 1.0.0
 */
public class KafkaUtil {

    public static Properties consumerProperties;
    public static Properties producerProperties;

    static {

        consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master01:9092,slave01:9092,slave02:9092");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "master01:9092,slave01:9092,slave02:9092");
    }
}
