package com.example.streams.topology;

import java.time.Duration;

import com.example.streams.model.DeviceEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeviceStatusTopology {
  private static final Logger log = LoggerFactory.getLogger(DeviceStatusProcessorSupplier.class);

  public static final String OFFLINE_TIMER_STORE = "offline-timer-store";

  public static Topology buildTopology(Serde<String> keySerde,
                                       Serde<DeviceEvent> valueSerde,
                                       String inputTopic,
                                       String outputTopic) {
    Topology topology = new Topology();

    // Add a persistent key-value store for offline timers
//    topology.addStateStore(
//        Stores.keyValueStoreBuilder(
//            Stores.persistentKeyValueStore(OFFLINE_TIMER_STORE),
//            keySerde,
//            org.apache.kafka.common.serialization.Serdes.Long()
//        )
//    );

    topology.addSource("device-status-input",
        keySerde.deserializer(),
        valueSerde.deserializer(),
        inputTopic);
    topology.addProcessor("device-status-processor", new DeviceStatusProcessorSupplier(outputTopic), "device-status-input");
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(OFFLINE_TIMER_STORE),
            keySerde,
            org.apache.kafka.common.serialization.Serdes.Long()
        ),
        "device-status-processor"
    );
    topology.addSink("offline-finalized-sink", outputTopic, keySerde.serializer(), valueSerde.serializer(), "device-status-processor");

    return topology;
  }

  static class DeviceStatusProcessorSupplier implements ProcessorSupplier<String, DeviceEvent, String, DeviceEvent> {
    private final String outputTopic;

    DeviceStatusProcessorSupplier(String outputTopic) {
      this.outputTopic = outputTopic;
    }

    @Override
    public Processor<String, DeviceEvent, String, DeviceEvent> get() {
      return new Processor<>() {
        private ProcessorContext<String, DeviceEvent> context;
        private KeyValueStore<String, Long> store;

        @Override
        public void init(ProcessorContext<String, DeviceEvent> context) {
          this.context = context;
          this.store = context.getStateStore(OFFLINE_TIMER_STORE);

          log.info("Initialized device-status-processor; state store '{}' acquired", OFFLINE_TIMER_STORE);

          // Schedule punctuation every 60 seconds
          context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            long now = System.currentTimeMillis();
            try (KeyValueIterator<String, Long> it = store.all()) {
              while (it.hasNext()) {
                KeyValue<String, Long> kv = it.next();
                if (kv.value != null && now >= kv.value) {
                  // Emit OFFLINE_FINALIZED event
                  DeviceEvent finalized = new DeviceEvent(kv.key, "OFFLINE_FINALIZED", now);
                  context.forward(new Record<>(kv.key, finalized, now));
                  // Remove from store
                  store.delete(kv.key);
                }
              }
            }
          });
        }

        @Override
        public void process(Record<String, DeviceEvent> record) {
          if (record == null || record.value() == null) return;
          DeviceEvent event = record.value();
          String deviceId = event.deviceId();
          String status = event.status();
          long now = System.currentTimeMillis();

          if ("OFFLINE".equalsIgnoreCase(status)) {
            long expiry = now + 65_000L;
            store.put(deviceId, expiry);
            log.info("Registered OFFLINE timer for {} expiring at {}", deviceId, expiry);
          } else if ("ONLINE".equalsIgnoreCase(status)) {
            store.delete(deviceId);
            log.info("Cleared timer for {} due to ONLINE");
          }

        }

        @Override
        public void close() {
        }
      };
    }
  }
}
