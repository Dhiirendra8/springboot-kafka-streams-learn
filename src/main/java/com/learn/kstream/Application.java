package com.learn.kstream;

import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.NullChannel;
import org.springframework.messaging.SubscribableChannel;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;
import java.util.function.Function;

@SpringBootApplication
public class Application {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public static void main(String[] args) {
        SpringApplication.run(Application.class);
    }

    @Bean
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Low-level API: define the stream
        KStream<String, String> stream = builder.stream("processor-input", Consumed.with(Serdes.String(), Serdes.String()));

        // Processing: convert value to uppercase
        stream
                .peek((key,value)->System.out.println("Topology: Records with Key= "+key +" : "+"Value= "+value))
                .mapValues(value -> value.toUpperCase())
                .to("processor-output", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
    @Bean
    public KafkaStreams kafkaStreams(Topology topology) {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-kstream-app");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();

        return streams;
    }

//    @Bean
//    public Function<KStream<String, String>, KStream<String, String>> process() {
//        return input -> input.peek((key, value) -> {
//            System.out.println("🟢 Received Key: " + key + ", Value: " + value);
//        });
//    }
}
