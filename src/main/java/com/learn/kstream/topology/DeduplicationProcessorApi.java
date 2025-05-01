package com.learn.kstream.topology;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;



public class DeduplicationProcessorApi implements Processor<String, String, String, String> {

    private final Logger log = LoggerFactory.getLogger(DeduplicationProcessorApi.class);

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> store;
    private String storeName = "dedup-store";

//    public DeduplicationProcessorApi(String storeName){
//        this.storeName = storeName;
//    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        store = context.getStateStore(storeName);
        Objects.requireNonNull(store, "State store can't be null");
    }

    @Override
    public void process(Record<String, String> record) {
        String key = record.key();
        String value = record.value();
        if(store.get(key) == null){
            log.info("Processor API - Unique found for key: {}", key + " : value=" + value);
            store.put(key, value);
            context.forward(record, "unique-sink");
        }else {
            log.info("Processor API - Duplicate found for key: {}", key + " : value=" + value);
            context.forward(record, "duplicate-sink");
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
