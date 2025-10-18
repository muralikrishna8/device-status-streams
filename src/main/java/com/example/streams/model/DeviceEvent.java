package com.example.streams.model;

/**
 * Device event representing device online/offline status.
 *
 * deviceId: Partition key for ordering.
 * status: "ONLINE" | "OFFLINE"
 * timestamp: event time in milliseconds.
 */
public record DeviceEvent(String deviceId, String status, Long timestamp) { }
