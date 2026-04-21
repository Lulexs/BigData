package com.bigdata.luka.kafkaproducer;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StartupRunner implements CommandLineRunner {

    private final EmissionXmlProcessor processor;

    @Override
    public void run(String... args) {
        processor.process();
    }
}
