package com.learn.kstream.processor;

import com.learn.kstream.model.WordCount;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

//@Component
public class MultipleOutputBranchingProcessor {
    @Bean
    public Function<KStream<Object, String>, KStream<?, WordCount>[]> multipleOutputBranching() {

        Predicate<Object, WordCount> isEnglish = (k, v) -> v.equals("english");
        Predicate<Object, WordCount> isFrench = (k, v) -> v.equals("french");
        Predicate<Object, WordCount> isSpanish = (k, v) -> v.equals("spanish");

        return input -> input
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .windowedBy(TimeWindows.of(Duration.of(5, ChronoUnit.SECONDS)))
                .count(Materialized.as("WordCounts-branch"))
                .toStream()
                .map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value,
                        new Date(key.window().start()), new Date(key.window().end()))))
                .branch(isEnglish, isFrench, isSpanish);
    }
}

