package com.learn.kstream.topology;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;

@Configuration
public class KafkaStreamsConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value(value = "${spring.kafka.streams.application-id}")
    private String applicationId;


//    @Bean(name = "kafkaStreamsUppercase")
//    public KafkaStreams kafkaStreamsUppercase(@Qualifier("topology-uppercase") Topology buildTopology) {
//        log.info("---Inside kafkaStreamsUppercase---");
//        return createKafkaStreams(buildTopology);
//    }

    @Bean(name = "kafkaStreamsDuplicate")
    public KafkaStreams kafkaStreamsDuplicate(@Qualifier("topology-duplicate") Topology duplicateTopology) {
        log.info("---Inside kafkaStreamsDuplicate---");
        return createKafkaStreams(duplicateTopology);
    }
    private KafkaStreams createKafkaStreams(Topology topology) {
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        return streams;
    }
    private Properties streamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
