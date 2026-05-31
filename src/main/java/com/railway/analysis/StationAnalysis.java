package com.railway.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class StationAnalysis {
    public static Dataset<Row> run(SparkSession spark){

        System.out.println("STATION ANALYSIS STARTED :) ");

        spark.sql("use railway_db");
        System.out.println("entered into railway_db!");

        // Read clean data from hive table
        Dataset<Row> stationsdf = spark.sql("select * from stations_clean");
        Dataset<Row> schedulesdf = spark.sql("select * from schedules_clean");

        // ANALYSIS 1 - Busiest stations by train count
        System.out.println("--- Busiest Stations by  Train count ---");

        Dataset<Row> busiestStations = schedulesdf
                .groupBy("station_code","station_name")
                .agg(
                        countDistinct("train_number").alias("train_count")
                )
                .orderBy(desc("train_count"))
                .limit(20);

        busiestStations.show();

        //ANALYSIS 2 - State Wise Station count
        System.out.println("--- state wise Station count ---");
        Dataset<Row> statewiseCount = stationsdf
                .groupBy("state")
                .agg(
                        count("station_code").alias("total_stations")
                )
                .orderBy(desc("total_stations"));

        statewiseCount.show();

        //ANALYSIS 3 - zone wise station count
        System.out.println("--- Zone wise Station count ---");
        Dataset<Row> zoneWithStations = stationsdf
                .groupBy("zone")
                .agg(
                        count("station_code").alias("total_stations")
                )
                .orderBy(desc("total_stations"));

        zoneWithStations.show();

        //ANALYSIS 4 - Final station summary
        // join schedules count with station details
        System.out.println("--- Final Station Summary ---");

        Dataset<Row> stationSummary = busiestStations
                .join(stationsdf,"station_code")
                .select(
                        col("station_code"),
                        stationsdf.col("station_name"),
                        col("state"),
                        col("zone"),
                        col("train_count")
                )
                .orderBy(desc("train_count"))
                .limit(20);

        stationSummary.show();

        System.out.println("STATION ANALYSIS COMPLETED!");

        return stationSummary;
    }
}
