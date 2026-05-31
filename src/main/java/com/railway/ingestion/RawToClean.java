package com.railway.ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class RawToClean {

    public static void run(SparkSession spark) {

        System.out.println("STEP 1 : RAW TO CLEAN STARTED :)");

        // ─────────────────────────────────────────
        // 1. READ TRAINS JSON FROM HDFS
        // ─────────────────────────────────────────
        System.out.println("Reading trains.json from HDFS...");

        Dataset<Row> trainsRaw = spark.read()
                .option("multiline", "true")
                .json("hdfs://localhost:50000/railway_prj/railway_raw/trains/trains.json");

        spark.sparkContext().setLogLevel("ERROR");

        // Explode features array first
        Dataset<Row> trainsExploded = trainsRaw
                .select(explode(col("features")).alias("feature"));

        // Select from feature.properties
        Dataset<Row> trainsdf = trainsExploded.select(
                        col("feature.properties.number").alias("train_number"),
                        col("feature.properties.name").alias("train_name"),
                        col("feature.properties.zone").alias("zone"),
                        col("feature.properties.type").alias("train_type"),
                        col("feature.properties.from_station_name").alias("from_station"),
                        col("feature.properties.to_station_name").alias("to_station"),
                        col("feature.properties.from_station_code").alias("from_code"),
                        col("feature.properties.to_station_code").alias("to_code"),
                        col("feature.properties.departure").alias("departure"),
                        col("feature.properties.arrival").alias("arrival"),
                        col("feature.properties.duration_h").alias("duration_hours"),
                        col("feature.properties.duration_m").alias("duration_mins"),
                        col("feature.properties.distance").alias("distance"),
                        col("feature.properties.sleeper").alias("sleeper"),
                        col("feature.properties.first_ac").alias("first_ac"),
                        col("feature.properties.second_ac").alias("second_ac"),
                        col("feature.properties.third_ac").alias("third_ac")
                )
                .filter(col("train_number").isNotNull())
                .filter(col("train_name").isNotNull())
                .filter(col("zone").isNotNull())
                .dropDuplicates("train_number");

        System.out.println("Valid Trains count : " + trainsdf.count());

        // Write to railway_clean HDFS
        trainsdf.write()
                .mode("overwrite")
                .parquet("hdfs://localhost:50000/railway_prj/railway_clean/trains/");

        System.out.println("Trains written to railway_clean...");

        // ─────────────────────────────────────────
        // 2. READ STATIONS JSON FROM HDFS
        // ─────────────────────────────────────────
        System.out.println("Reading stations.json from HDFS...");

        Dataset<Row> stationsRaw = spark.read()
                .option("multiline", "true")
                .json("hdfs://localhost:50000/railway_prj/railway_raw/stations/stations.json");

        // Explode features array first
        Dataset<Row> stationsExploded = stationsRaw
                .select(explode(col("features")).alias("feature"));

        // Select from feature.properties
        Dataset<Row> stationsdf = stationsExploded.select(
                        col("feature.properties.code").alias("station_code"),
                        col("feature.properties.name").alias("station_name"),
                        col("feature.properties.zone").alias("zone"),
                        col("feature.properties.state").alias("state"),
                        col("feature.properties.address").alias("address"),
                        col("feature.geometry.coordinates").alias("coordinates")
                )
                .filter(col("station_code").isNotNull())
                .filter(col("station_name").isNotNull())
                .dropDuplicates("station_code");

        System.out.println("Stations valid count : " + stationsdf.count());

        // Write to railway_clean HDFS
        stationsdf.write()
                .mode("overwrite")
                .parquet("hdfs://localhost:50000/railway_prj/railway_clean/stations/");

        System.out.println("Stations written to railway_clean...");

        // ─────────────────────────────────────────
        // 3. READ SCHEDULES JSON FROM HDFS
        // ─────────────────────────────────────────
        System.out.println("Reading schedules.json from HDFS...");

        Dataset<Row> schedulesdf = spark.read()
                .option("multiline", "true")
                .json("hdfs://localhost:50000/railway_prj/railway_raw/schedules/schedules.json")
                .filter(col("train_number").isNotNull())
                .filter(col("station_code").isNotNull())
                .filter(col("arrival").notEqual("None"))
                .dropDuplicates("id");

        System.out.println("Schedules clean count : " + schedulesdf.count());

        // Write to railway_clean HDFS
        schedulesdf.write()
                .mode("overwrite")
                .parquet("hdfs://localhost:50000/railway_prj/railway_clean/schedules/");

        System.out.println("Schedules written to railway_clean...");

        // ─────────────────────────────────────────
        // 4. CREATE HIVE TABLES ON CLEAN DATA
        // ─────────────────────────────────────────
        System.out.println("Creating Hive tables...");

        spark.sql("CREATE DATABASE IF NOT EXISTS railway_db");

        // Trains Hive table
        trainsdf.write()
                .mode("overwrite")
                .saveAsTable("railway_db.trains_clean");

        System.out.println("trains_clean Hive table created ✓");

        // Stations Hive table
        stationsdf.write()
                .mode("overwrite")
                .saveAsTable("railway_db.stations_clean");

        System.out.println("stations_clean Hive table created ✓");

        // Schedules Hive table
        schedulesdf.write()
                .mode("overwrite")
                .saveAsTable("railway_db.schedules_clean");

        System.out.println("schedules_clean Hive table created ✓");

        System.out.println("STEP 1 : RAW TO CLEAN COMPLETED :)");
    }
}