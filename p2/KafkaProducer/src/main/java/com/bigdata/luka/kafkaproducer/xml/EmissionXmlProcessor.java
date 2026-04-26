package com.bigdata.luka.kafkaproducer.xml;

import com.bigdata.luka.kafkaproducer.model.VehicleEmissionEvent;
import com.bigdata.luka.kafkaproducer.producers.KafkaEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmissionXmlProcessor {

    private static final String TOPIC = "EMISSIONS";

    private final KafkaEventProducer eventProducer;

    @Async
    public void process() {
        XMLInputFactory factory = XMLInputFactory.newInstance();

        try (InputStream inputStream = new ClassPathResource("emissions.xml").getInputStream()) {
            XMLStreamReader reader = factory.createXMLStreamReader(inputStream);

            BigDecimal currentTime = null;
            List<VehicleEmissionEvent> currentBatch = new ArrayList<>();
            long totalCount = 0;

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT) {
                    String localName = reader.getLocalName();

                    if ("timestep".equals(localName)) {
                        if (currentTime != null && !currentBatch.isEmpty()) {
                            publishBatch(currentTime, currentBatch);
                            totalCount += currentBatch.size();
                            Thread.sleep(1000);
                            currentBatch = new ArrayList<>();
                        }

                        currentTime = decimalAttr(reader, "time");
                    } else if ("vehicle".equals(localName)) {
                        VehicleEmissionEvent payload = VehicleEmissionEvent.builder()
                                .timestep(currentTime)
                                .id(stringAttr(reader, "id"))
                                .eclass(stringAttr(reader, "eclass"))
                                .co2(decimalAttr(reader, "CO2"))
                                .co(decimalAttr(reader, "CO"))
                                .hc(decimalAttr(reader, "HC"))
                                .nox(decimalAttr(reader, "NOx"))
                                .pmx(decimalAttr(reader, "PMx"))
                                .fuel(decimalAttr(reader, "fuel"))
                                .electricity(decimalAttr(reader, "electricity"))
                                .noise(decimalAttr(reader, "noise"))
                                .route(stringAttr(reader, "route"))
                                .type(stringAttr(reader, "type"))
                                .waiting(decimalAttr(reader, "waiting"))
                                .lane(stringAttr(reader, "lane"))
                                .pos(decimalAttr(reader, "pos"))
                                .speed(decimalAttr(reader, "speed"))
                                .angle(decimalAttr(reader, "angle"))
                                .x(decimalAttr(reader, "x"))
                                .y(decimalAttr(reader, "y"))
                                .build();

                        currentBatch.add(payload);
                    }
                }
            }

            if (currentTime != null && !currentBatch.isEmpty()) {
                publishBatch(currentTime, currentBatch);
                totalCount += currentBatch.size();
            }

            reader.close();
            log.info("Finished emissions processing. Published {} records", totalCount);

        } catch (Exception e) {
            log.error("Error while processing emissions XML", e);
        }
    }

    private void publishBatch(BigDecimal timestep, List<VehicleEmissionEvent> batch) {
        log.info("Publishing EMISSION timestep={} with {} vehicles", timestep, batch.size());

        for (VehicleEmissionEvent payload : batch) {
            eventProducer.send(payload, TOPIC);
        }
    }

    private String stringAttr(XMLStreamReader reader, String attrName) {
        return reader.getAttributeValue(null, attrName);
    }

    private BigDecimal decimalAttr(XMLStreamReader reader, String attrName) {
        String value = reader.getAttributeValue(null, attrName);
        return value == null || value.isBlank() ? null : new BigDecimal(value);
    }
}