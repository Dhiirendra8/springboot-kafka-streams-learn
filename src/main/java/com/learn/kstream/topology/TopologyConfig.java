package com.learn.kstream.topology;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
public class TopologyConfig {
    private static final Logger log = LoggerFactory.getLogger(TopologyConfig.class);

    private final Deserializer<String> keyDeSerializer = new StringDeserializer();

    private final Deserializer<String> valueDeSerializer =
            new StringDeserializer();

    private final Serde<String> keySerializer = Serdes.String();

    private final Serde<String> valueSerializer = Serdes.String();

    private static final String storeName = "dedup-store";
//    @Bean
//    @Qualifier("topology-uppercase")
//    public Topology topologyUppercase() {
//        StreamsBuilder builder = new StreamsBuilder();
//
//        // Low-level API: define the stream
//        KStream<String, String> stream = builder.stream("processor-input", Consumed.with(Serdes.String(), Serdes.String()));
//
//        // Processing: convert value to uppercase
//        stream
//                .peek((key,value)->System.out.println("Topology: Records with Key= "+key +" : "+"Value= "+value))
//                .mapValues(value -> value.toUpperCase())
//                .to("processor-output", Produced.with(Serdes.String(), Serdes.String()));
//
//        return builder.build();
//    }


    private final ObjectFactory<DeduplicationProcessor> duplicationProcessorObjectFactory;
    public TopologyConfig(ObjectFactory<DeduplicationProcessor> duplicationProcessorObjectFactory) {
        this.duplicationProcessorObjectFactory = duplicationProcessorObjectFactory;
    }

    public DeduplicationProcessor getDeduplicationProcessor() {
        return duplicationProcessorObjectFactory.getObject();
    }
    @Bean
    @Qualifier("topology-duplicate")
    public Topology topologyDuplicate() {
        // Name of the state store
        String storeName = "dedup-store";

        // Create a persistent key-value store supplier
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);

        // Build the topology using the Processor API

        Topology topology = new StreamsBuilder().build();

        // Add the source to the topology
        topology.addSource("source-node", keyDeSerializer, valueDeSerializer, "dedupe-input-topic")
///                .addProcessor("deduplication-processor", this::getDeduplicationProcessor, "source-node") // Add custom processor
                .addProcessor("deduplication-processor", DeduplicationProcessorApi::new, "source-node") // Add custom processor
                .addStateStore(Stores.keyValueStoreBuilder(storeSupplier, keySerializer, valueSerializer), "deduplication-processor") // Add state store to the processor node
                .addSink("duplicate-sink", "duplicates-topic", "deduplication-processor") // Output to duplicates-topic
                .addSink("unique-sink", "uniques-topic", "deduplication-processor"); // Output to uniques-topic

        log.info("Finished Topology Creation:\n{}", topology.describe());
        return topology;
    }

}
