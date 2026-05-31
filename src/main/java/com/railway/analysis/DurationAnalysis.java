package com.railway.analysis;

import com.railway.udf.JourneyClassifierUDF;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class DurationAnalysis {
    public static Dataset<Row> run(SparkSession spark){
        System.out.println("DURATION ANALYSIS STARTED :)");

        // Register UDF
        spark.udf().register("JourneyClassifier",new JourneyClassifierUDF(),DataTypes.StringType);

        System.out.println("JourneyClassifierUDF registered!");

        // read clean trains from hive table
        Dataset<Row> trainsDF = spark.sql("select * from railway_db.trains_clean");

        //ANALYSIS 1 - Calculated total minutes and apply UDF to Classify journey
        System.out.println("--- Journey Classification ---");

        Dataset<Row> classifiedDF = trainsDF
                .filter(col("duration_hours").isNotNull())
                .filter(col("duration_mins").isNotNull())
                .withColumn("total_minutes",col("duration_hours").multiply(60)
                        .plus(col("duration_mins")))
                .withColumn("journey_class",
                        callUDF("journeyClassifier", col("total_minutes")))
                .select(
                        col("train_number"),
                        col("train_name"),
                        col("duration_hours"),
                        col("duration_mins"),
                        col("total_minutes"),
                        col("journey_class"),
                        col("zone")
                );

        classifiedDF.show(10, false);

        // ANALYSIS 2 - Journey class Distribution
        // How Many trains in each class
        System.out.println("--- Journey Class Distribution ---");

        Dataset<Row> classDistribution = classifiedDF
                .groupBy("journey_class")
                .agg(
                        count("train_number").alias("total_trains")
                )
                .orderBy(desc("total_trains"));

        classDistribution.show();

        // ANALYSIS 3 - Top 10 longest journeys
        System.out.println("--- Top 10 Longest Journeys ---");

        Dataset<Row> longestJourneys = classifiedDF
                .orderBy(desc("total_minutes"))
                .limit(10);

        longestJourneys.show(false);

        // ANALYSIS 4 - Zone wise avg duration
        System.out.println("--- Zone wise Average Duration ---");

        Dataset<Row> zoneAvgDuration = classifiedDF
                .groupBy("zone")
                .agg(
                        round(avg("total_minutes"), 2).alias("avg_minutes"),
                        max("total_minutes").alias("max_minutes"),
                        min("total_minutes").alias("min_minutes")
                )
                .orderBy(desc("avg_minutes"));

        zoneAvgDuration.show();

        System.out.println("   DURATION ANALYSIS COMPLETED");

        return classifiedDF;


    }

}
