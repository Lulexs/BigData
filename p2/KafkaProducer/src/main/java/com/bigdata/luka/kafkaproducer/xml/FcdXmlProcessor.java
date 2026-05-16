package com.bigdata.luka.kafkaproducer.xml;

import com.bigdata.luka.kafkaproducer.model.VehicleFcdEvent;
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
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class FcdXmlProcessor {

    private static final String TOPIC = "FCD";

    private final KafkaEventProducer eventProducer;

    @Async
    public void process() {
        XMLInputFactory factory = XMLInputFactory.newInstance();

        try (InputStream inputStream = new ClassPathResource("fcd.xml").getInputStream()) {
            XMLStreamReader reader = factory.createXMLStreamReader(inputStream);

            Long currentBaseTime = LocalDateTime.of(2026, Month.MAY, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
            Long currentTime = null;
            List<VehicleFcdEvent> currentBatch = new ArrayList<>();
            long totalCount = 0;

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT) {
                    String localName = reader.getLocalName();

                    if ("timestep".equals(localName)) {
                        if (currentTime != null && !currentBatch.isEmpty()) {
                            publishBatch(currentTime, currentBatch);
                            totalCount += currentBatch.size();
//                            Thread.sleep(1000);
                            currentBatch = new ArrayList<>();
                        }

                        currentTime = currentBaseTime + decimalAttr(reader, "time").longValue() * 1000;
                    } else if ("vehicle".equals(localName)) {
                        VehicleFcdEvent payload = VehicleFcdEvent.builder()
                                .timestep(currentTime)
                                .id(stringAttr(reader, "id"))
                                .x(decimalAttr(reader, "x"))
                                .y(decimalAttr(reader, "y"))
                                .angle(decimalAttr(reader, "angle"))
                                .type(stringAttr(reader, "type"))
                                .speed(decimalAttr(reader, "speed"))
                                .pos(decimalAttr(reader, "pos"))
                                .lane(stringAttr(reader, "lane"))
                                .slope(decimalAttr(reader, "slope"))
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
            log.info("Finished FCD processing. Published {} records", totalCount);

        } catch (Exception e) {
            log.error("Error while processing FCD XML", e);
        }
    }

    private void publishBatch(Long timestep, List<VehicleFcdEvent> batch) {
        log.info("Publishing FCD timestep={} with {} vehicles", timestep, batch.size());

        for (VehicleFcdEvent payload : batch) {
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