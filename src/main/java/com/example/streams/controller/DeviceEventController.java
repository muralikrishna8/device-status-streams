package com.example.streams.controller;

import com.example.streams.model.DeviceEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Locale;

@RestController
@RequestMapping("/api")
@Validated
public class DeviceEventController {

    private final KafkaTemplate<String, DeviceEvent> kafkaTemplate;
    private final String inputTopic;

    public DeviceEventController(KafkaTemplate<String, DeviceEvent> kafkaTemplate,
                                 @Value("${cloud.stream.bindings.input.destination}") String inputTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.inputTopic = inputTopic;
    }

    @PostMapping("/events")
    public ResponseEntity<?> publishEvent(@Valid @RequestBody DeviceEvent event) {
        if (event == null || event.deviceId() == null || event.deviceId().isBlank()) {
            return ResponseEntity.badRequest().body("deviceId is required");
        }
        if (event.status() == null) {
            return ResponseEntity.badRequest().body("status is required");
        }
        String status = event.status().toUpperCase(Locale.ROOT);
        if (!("ONLINE".equals(status) || "OFFLINE".equals(status))) {
            return ResponseEntity.badRequest().body("status must be either ONLINE or OFFLINE");
        }

        DeviceEvent toSend = event.timestamp() == null
                ? new DeviceEvent(event.deviceId(), status, System.currentTimeMillis())
                : new DeviceEvent(event.deviceId(), status, event.timestamp());

        kafkaTemplate.send(inputTopic, toSend.deviceId(), toSend);
        return ResponseEntity.accepted().build();
    }
}
