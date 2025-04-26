package com.learn.kstream.processor;

import com.learn.kstream.model.WordCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

@Component
public class SingleInputOutputProcessor {
    @Bean
    public Function<KStream<String, String>, KStream<String, WordCount>> singleInputOutput() {

        return input -> input
                .peek((key,value)->System.out.println("singleInputOutput: Records with Key= "+key +" : "+"Value= "+value))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .map((key, value) -> new KeyValue<>(value, value))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(Duration.of(5, ChronoUnit.SECONDS)))
                .count(Materialized.as("wordcount-store"))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), new WordCount(key.key(), value,
                        new Date(key.window().start()), new Date(key.window().end()))));
    }
}
