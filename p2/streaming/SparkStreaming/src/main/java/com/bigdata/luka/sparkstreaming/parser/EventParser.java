package com.bigdata.luka.sparkstreaming.parser;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public final class EventParser {

    private EventParser() {
    }

    public static Dataset<Row> parseEmissions(Dataset<Row> kafkaDf) {
        StructType emissionsSchema = new StructType()
                .add("timestep", DataTypes.DoubleType)
                .add("id", DataTypes.StringType)
                .add("eclass", DataTypes.StringType)
                .add("co2", DataTypes.DoubleType)
                .add("co", DataTypes.DoubleType)
                .add("hc", DataTypes.DoubleType)
                .add("nox", DataTypes.DoubleType)
                .add("pmx", DataTypes.DoubleType)
                .add("fuel", DataTypes.DoubleType)
                .add("electricity", DataTypes.DoubleType)
                .add("noise", DataTypes.DoubleType)
                .add("route", DataTypes.StringType)
                .add("type", DataTypes.StringType)
                .add("waiting", DataTypes.DoubleType)
                .add("lane", DataTypes.StringType)
                .add("pos", DataTypes.DoubleType)
                .add("speed", DataTypes.DoubleType)
                .add("angle", DataTypes.DoubleType)
                .add("x", DataTypes.DoubleType)
                .add("y", DataTypes.DoubleType)
                .add("key", DataTypes.StringType)
                .add("timestamp", DataTypes.DoubleType)
                .add("vehicleId", DataTypes.StringType);

        return kafkaDf
                .filter(col("topic").equalTo("EMISSIONS"))
                .select(
                        col("topic"),
                        col("timestamp").alias("kafkaTimestamp"),
                        col("value").cast("string").alias("json")
                )
                .withColumn("data", from_json(col("json"), emissionsSchema))
                .select(
                        col("topic").alias("sourceTopic"),
                        col("kafkaTimestamp").alias("eventTime"),

                        col("data.timestep").alias("timestep"),
                        col("data.timestamp").alias("timestamp"),

                        col("data.id").alias("id"),
                        col("data.vehicleId").alias("vehicleId"),
                        col("data.key").alias("key"),

                        col("data.eclass").alias("eclass"),
                        col("data.type").alias("vehicleType"),

                        col("data.co2").alias("co2"),
                        col("data.co").alias("co"),
                        col("data.hc").alias("hc"),
                        col("data.nox").alias("nox"),
                        col("data.pmx").alias("pmx"),
                        col("data.fuel").alias("fuel"),
                        col("data.electricity").alias("electricity"),
                        col("data.noise").alias("noise"),

                        col("data.route").alias("route"),
                        col("data.waiting").alias("waiting"),
                        col("data.lane").alias("lane"),
                        col("data.pos").alias("pos"),
                        col("data.speed").alias("speed"),
                        col("data.angle").alias("angle"),

                        col("data.x").alias("x"),
                        col("data.y").alias("y")
                );
    }

    public static Dataset<Row> parseFcd(Dataset<Row> kafkaDf) {
        StructType fcdSchema = new StructType()
                .add("timestep", DataTypes.DoubleType)
                .add("id", DataTypes.StringType)
                .add("x", DataTypes.DoubleType)
                .add("y", DataTypes.DoubleType)
                .add("angle", DataTypes.DoubleType)
                .add("type", DataTypes.StringType)
                .add("speed", DataTypes.DoubleType)
                .add("pos", DataTypes.DoubleType)
                .add("lane", DataTypes.StringType)
                .add("slope", DataTypes.DoubleType)
                .add("key", DataTypes.StringType)
                .add("timestamp", DataTypes.DoubleType)
                .add("vehicleId", DataTypes.StringType);

        return kafkaDf
                .filter(col("topic").equalTo("FCD"))
                .select(
                        col("topic"),
                        col("timestamp").alias("kafkaTimestamp"),
                        col("value").cast("string").alias("json")
                )
                .withColumn("data", from_json(col("json"), fcdSchema))
                .select(
                        col("topic").alias("sourceTopic"),
                        col("kafkaTimestamp").alias("eventTime"),

                        col("data.timestep").alias("timestep"),
                        col("data.timestamp").alias("timestamp"),

                        col("data.id").alias("id"),
                        col("data.vehicleId").alias("vehicleId"),
                        col("data.key").alias("key"),

                        col("data.type").alias("vehicleType"),
                        col("data.angle").alias("angle"),
                        col("data.speed").alias("speed"),
                        col("data.pos").alias("pos"),
                        col("data.lane").alias("lane"),
                        col("data.slope").alias("slope"),

                        col("data.x").alias("lon"),
                        col("data.y").alias("lat")
                );
    }
}