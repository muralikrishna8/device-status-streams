package com.example.streams.config;

import com.example.streams.model.DeviceEvent;
import com.example.streams.topology.DeviceStatusTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class KafkaStreamsConfig {

    @Bean
    public JsonSerde<DeviceEvent> deviceEventJsonSerde() {
        // Disable type headers to be consistent with producer config
        JsonSerde<DeviceEvent> serde = new JsonSerde<>(DeviceEvent.class);
        serde.noTypeInfo();
        return serde;
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
}
