package com.example.streams.config;

import com.example.streams.model.DeviceEvent;
import com.example.streams.topology.DeviceStatusTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

import org.apache.kafka.streams.StreamsConfig;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamsConfig {

    @Bean
    public JsonSerde<DeviceEvent> deviceEventJsonSerde() {
        // Disable type headers to be consistent with producer config
        JsonSerde<DeviceEvent> serde = new JsonSerde<>(DeviceEvent.class);
        serde.noTypeInfo();
        return serde;
    }

    // Provide a default KafkaStreamsConfiguration if Boot doesn't auto-create one
    @Bean(name = "defaultKafkaStreamsConfig")
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(KafkaProperties properties) {
        Map<String, Object> props = new HashMap<>();
        // Required basics
        String appId = properties.getStreams().getApplicationId();
        if (appId == null || appId.isBlank()) {
            appId = properties.getClientId() != null ? properties.getClientId() : "device-status-processor";
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        // Include user-provided additional properties
        if (properties.getProperties() != null) {
            props.putAll(properties.getProperties());
        }
        if (properties.getStreams() != null && properties.getStreams().getProperties() != null) {
            props.putAll(properties.getStreams().getProperties());
        }
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public Topology deviceStatusTopology(
            JsonSerde<DeviceEvent> deviceEventSerde,
            @Value("${cloud.stream.bindings.input.destination}") String inputTopic,
            @Value("${cloud.stream.bindings.output.destination}") String outputTopic
    ) {
        Serde<String> keySerde = Serdes.String();
        return DeviceStatusTopology.buildTopology(keySerde, deviceEventSerde, inputTopic, outputTopic);
    }

    // Ensure the topology actually starts by creating and starting a KafkaStreams instance
    @Bean(destroyMethod = "close")
    public KafkaStreams kafkaStreams(Topology topology, @Qualifier("defaultKafkaStreamsConfig") KafkaStreamsConfiguration streamsConfig) {
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig.asProperties());
        streams.start();
        return streams;
    }
}
