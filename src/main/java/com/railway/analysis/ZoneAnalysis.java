package com.railway.analysis;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;

public class ZoneAnalysis {

    public static Dataset<Row> run(SparkSession spark){
        System.out.println("ZONE ANALYSIS STARTED :)");

        spark.sql("use railway_db");

        System.out.println("Entered into railway_db!");

        // Read clean trains from hive tables
        Dataset<Row> trainsdf = spark.sql("select * from trains_clean");

        // Read clean stations from hive table
        Dataset<Row> stationdf = spark.sql("select * from stations_clean");

        // ANALYSIS 1 - ZONE wise Train count
        System.out.println("--- Zone wise Train Count ---");

        Dataset<Row> zoneWiseTrainCntDF = trainsdf
                .groupBy("zone")
                .agg(
                        count("train_number").alias("total_trains"),
                        round(avg("distance"),2).alias("avg_distance")
                )
                .orderBy(desc("total_trains"));

        zoneWiseTrainCntDF.show();

        // ANALYSIS 2 - Zone wise station count
        System.out.println("--- zone wise station count ---");

        Dataset<Row> zoneWiseStationCntDF = stationdf
                .groupBy("zone")
                .agg(
                        count("station_code").alias("total_stations")
                )
                .orderBy(desc("total_stations"));

        zoneWiseStationCntDF.show();

        // ANALYSIS 3 - Zone wise Train type breakdown
        System.out.println("--- Zone wise train type breakdown ---");

        Dataset<Row> zoneWisetrainTypeDF = trainsdf
                .groupBy("zone","train_type")
                .agg(
                        count("train_number").alias("type_count")
                )
                .orderBy(desc("type_count"));

        zoneWisetrainTypeDF.show();

        // ANALYSIS 4 - Final zone summary
        // Joining trains + stations zone data

        System.out.println("--- Final Zone Summary ---");

        Dataset<Row> zoneSummaryDF = zoneWiseTrainCntDF
                .join(zoneWiseStationCntDF,"zone")
                .select(
                        col("zone"),
                        col("total_trains"),
                        col("total_stations"),
                        col("avg_distance")
                )
                .orderBy(desc("total_trains"));

        zoneSummaryDF.show();

        System.out.println("ZONE ANALYSIS COMPLETED :)");

        return zoneSummaryDF;

    }

}
