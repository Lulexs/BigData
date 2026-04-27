package com.bigdata.luka.flinkstreaming.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class UnifiedEventParser extends ProcessFunction<String, Void> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void processElement(
            String value,
            Context ctx,
            Collector<Void> out) throws Exception {

        JsonNode root = MAPPER.readTree(value);

        String topic = root.get("topic").asText();
        long kafkaTimestamp = root.get("timestamp").asLong();

        JsonNode data = MAPPER.readTree(root.get("value").asText());

        switch (topic) {

            case "EMISSIONS":
                EmissionEvent e = new EmissionEvent();

                e.sourceTopic = "EMISSIONS";
                e.eventTime = kafkaTimestamp;

                e.timestep = getDouble(data, "timestep");
                e.timestamp = getDouble(data, "timestamp");

                e.id = getText(data, "id");
                e.vehicleId = getText(data, "vehicleId");
                e.key = getText(data, "key");

                e.eclass = getText(data, "eclass");
                e.vehicleType = getText(data, "type");

                e.co2 = getDouble(data, "co2");
                e.co = getDouble(data, "co");
                e.hc = getDouble(data, "hc");
                e.nox = getDouble(data, "nox");
                e.pmx = getDouble(data, "pmx");
                e.fuel = getDouble(data, "fuel");
                e.electricity = getDouble(data, "electricity");
                e.noise = getDouble(data, "noise");

                e.route = getText(data, "route");
                e.waiting = getDouble(data, "waiting");
                e.lane = getText(data, "lane");
                e.pos = getDouble(data, "pos");
                e.speed = getDouble(data, "speed");
                e.angle = getDouble(data, "angle");

                e.x = getDouble(data, "x");
                e.y = getDouble(data, "y");

                ctx.output(ParserOutputs.EMISSIONS, e);
                break;

            case "FCD":
                FcdEvent f = new FcdEvent();

                f.sourceTopic = "FCD";
                f.eventTime = kafkaTimestamp;

                f.timestep = getDouble(data, "timestep");
                f.timestamp = getDouble(data, "timestamp");

                f.id = getText(data, "id");
                f.vehicleId = getText(data, "vehicleId");
                f.key = getText(data, "key");

                f.vehicleType = getText(data, "type");
                f.angle = getDouble(data, "angle");
                f.speed = getDouble(data, "speed");
                f.pos = getDouble(data, "pos");
                f.lane = getText(data, "lane");
                f.slope = getDouble(data, "slope");

                f.lon = getDouble(data, "x");
                f.lat = getDouble(data, "y");

                ctx.output(ParserOutputs.FCD, f);
                break;

            default:
        }
    }

    private Double getDouble(JsonNode node, String field) {
        return node.has(field) ? node.get(field).asDouble() : null;
    }

    private String getText(JsonNode node, String field) {
        return node.has(field) ? node.get(field).asText() : null;
    }
}