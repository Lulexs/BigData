package com.bigdata.luka.kafkaproducer;

import com.bigdata.luka.kafkaproducer.xml.EmissionXmlProcessor;
import com.bigdata.luka.kafkaproducer.xml.FcdXmlProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StartupRunner implements CommandLineRunner {

    private final EmissionXmlProcessor emissionXmlProcessor;
    private final FcdXmlProcessor fcdXmlProcessor;

    @Override
    public void run(String... args) {
        fcdXmlProcessor.process();
        emissionXmlProcessor.process();
    }
}
