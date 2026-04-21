package com.bigdata.luka.kafkaproducer;
import com.bigdata.luka.kafkaproducer.model.VehicleEmissionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.math.BigDecimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmissionXmlProcessor {

    private final KafkaTemplate<String, VehicleEmissionEvent> kafkaTemplate;

    private static final String TOPIC = "EMISSIONS";

    public void process() {
        XMLInputFactory factory = XMLInputFactory.newInstance();

        try (InputStream inputStream = new ClassPathResource("emissions.xml").getInputStream()) {
            XMLStreamReader reader = factory.createXMLStreamReader(inputStream);

            BigDecimal currentTime = null;
            long count = 0;

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT) {
                    String localName = reader.getLocalName();

                    if ("timestep".equals(localName)) {
                        currentTime = decimalAttr(reader, "time");
                    }

                    if ("vehicle".equals(localName)) {
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

                        kafkaTemplate.send(TOPIC, payload.getId(), payload)
                                .whenComplete((result, ex) -> {
                                    if (ex != null) {
                                        log.error("Failed to publish vehicleId={}, timestep={}",
                                                payload.getId(), payload.getTimestep(), ex);
                                    } else {
                                        log.info("Published vehicleId={}, timestep={}, offset={}",
                                                payload.getId(),
                                                payload.getTimestep(),
                                                result.getRecordMetadata().offset());
                                    }
                                });
                        count++;
                    }
                }
            }

            reader.close();
            log.info("Finished processing XML. Published {} records", count);

        } catch (Exception e) {
            log.error("Error while processing XML file from classpath: emissions.xml", e);
        }
    }

    private String stringAttr(XMLStreamReader reader, String attrName) {
        return reader.getAttributeValue(null, attrName);
    }

    private BigDecimal decimalAttr(XMLStreamReader reader, String attrName) {
        String value = reader.getAttributeValue(null, attrName);
        if (value == null || value.isBlank()) {
            return null;
        }
        return new BigDecimal(value);
    }
}