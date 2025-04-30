package com.learn.kstream.topology;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Component
public class DeduplicationProcessor implements Processor<String, String> {

    private final Logger log = LoggerFactory.getLogger(DeduplicationProcessor.class);

    private org.apache.kafka.streams.processor.ProcessorContext context;
    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        store = context.getStateStore("dedup-store");
        Objects.requireNonNull(store, "State store can't be null");
    }

    @Override
    public void process(String key, String value) {
//        log.info("Inside process: Key=" + key + " : value=" + value);
        if (store.get(key) != null) {
            // Duplicate
            log.info("Duplicate found for key: {}", key + " : value=" + value);
            context.forward(key, value, To.child("duplicate-sink"));
        } else {
            // Unique
            log.info("Unique record for key: {}", key + " : value=" + value);
            store.put(key, value);
            context.forward(key, value, To.child("unique-sink"));

        }
    }

    @Override
    public void close() {

    }
}
