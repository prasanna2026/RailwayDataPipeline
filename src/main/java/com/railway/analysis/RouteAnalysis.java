package com.railway.analysis;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class RouteAnalysis {
    public static Dataset<Row> run(SparkSession spark){
        System.out.println("ROUTE ANALYSIS STARTED :)");

        // read clean data from hive tables;
        Dataset<Row> trainsDF = spark.sql("select * from railway_db.trains_clean");

        //ANALYSIS 1 - TOP 10 Longest routes
        System.out.println("--- Top 10 Longest Route ---");
        Dataset<Row> longestrouteDF = trainsDF
                .select(
                        col("train_number"),
                        col("train_name"),
                        col("from_station"),
                        col("to_station"),
                        col("distance"),
                        col("zone")
                );

        longestrouteDF.filter(col("distance").isNotNull())
                .orderBy(desc("distance"))
                .limit(10);

        longestrouteDF.show(false);

        //ANALYSIS 2 - Top 10 Shorted Routes
        System.out.println("--- Top 10 Shorted Routes ---");
        Dataset<Row> shortestrouteDF = trainsDF
                .select(
                        col("train_number"),
                        col("train_name"),
                        col("from_station"),
                        col("to_station"),
                        col("distance"),
                        col("zone")
                );
        shortestrouteDF.filter(col("distance").isNotNull())
                .filter(col("distance").gt(0))
                .orderBy(asc("distance"))
                .limit(10);

        shortestrouteDF.show(false);

        // ANALYSIS 3 - Zone wise avg Route distance
        System.out.println("--- zone wise Average Route distance ---");
        Dataset<Row> zoneAvgDistanceDF = trainsDF.groupBy("zone")
                .agg(
                        round(avg("distance"),2).alias("avg_distance"),
                        max("distance").alias("max_distance"),
                        min("distance").alias("min_distance"),
                        count("train_number").alias("total_trains")
                )
                .orderBy(desc("avg_distance"));

        zoneAvgDistanceDF.show();

        //ANALYSIS 4 - Final route Summary
        // Top 20 Longest Routes with full Summary
        System.out.println("--- Final Route Summary ---");

        Dataset<Row> routeSummary = trainsDF
                .select(
                        col("train_number"),
                        col("train_name"),
                        col("from_station"),
                        col("to_station"),
                        col("distance"),
                        col("zone")
                )
                .filter(col("distance").isNotNull())
                .filter(col("distance").gt(0))
                .orderBy(desc("distance"))
                .limit(20);

        routeSummary.show(false);

        System.out.println("   ROUTE ANALYSIS COMPLETED");

        return routeSummary;



    }
}
