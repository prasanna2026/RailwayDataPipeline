package com.railway.main;

import com.railway.ingestion.RawToClean;
import com.railway.analysis.ZoneAnalysis;
import com.railway.analysis.StationAnalysis;
import com.railway.analysis.RouteAnalysis;
import com.railway.analysis.DurationAnalysis;
import com.railway.output.MySQLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RailwayMain {

    public static void main(String[] args) {



        System.out.println("========================================");
        System.out.println("   RAILWAY DELAY INTELLIGENCE SYSTEM");
        System.out.println("   STARTED");
        System.out.println("========================================");

        // ─────────────────────────────────────────
        // CREATE SPARK SESSION
        // ─────────────────────────────────────────

        SparkSession spark = SparkSession.builder()
                .appName("RailwayDelayIntelligence")
                .master("local[*]")
                .config("spark.sql.warehouse.dir",
                        "hdfs://localhost:50000/user/hive/warehouse")
                .config("spark.hadoop.fs.defaultFS",
                        "hdfs://localhost:50000")
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("Spark Session Created ✓");

        try {

            // ─────────────────────────────────────────
            // STEP 1 - RAW TO CLEAN
            // ─────────────────────────────────────────
            RawToClean.run(spark);

            // ─────────────────────────────────────────
            // STEP 2 - ZONE ANALYSIS
            // ─────────────────────────────────────────
            Dataset<Row> zoneSummary = ZoneAnalysis.run(spark);

            // ─────────────────────────────────────────
            // STEP 3 - STATION ANALYSIS
            // ─────────────────────────────────────────
            Dataset<Row> stationSummary = StationAnalysis.run(spark);

            // ─────────────────────────────────────────
            // STEP 4 - ROUTE ANALYSIS
            // ─────────────────────────────────────────
            Dataset<Row> routeSummary = RouteAnalysis.run(spark);

            // ─────────────────────────────────────────
            // STEP 5 - DURATION ANALYSIS
            // ─────────────────────────────────────────
            Dataset<Row> durationSummary = DurationAnalysis.run(spark);

            // ─────────────────────────────────────────
            // STEP 6 - WRITE TO MYSQL
            // ─────────────────────────────────────────
            System.out.println("========================================");
            System.out.println("   WRITING RESULTS TO MYSQL");
            System.out.println("========================================");

            MySQLWriter.writeZoneAnalysis(zoneSummary);
            MySQLWriter.writeStationAnalysis(stationSummary);
            MySQLWriter.writeRouteAnalysis(routeSummary);
            MySQLWriter.writeDurationAnalysis(durationSummary);

            System.out.println("========================================");
            System.out.println("   ALL RESULTS WRITTEN TO MYSQL ✓");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("ERROR : " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Stop Spark at very end
            spark.stop();
            System.out.println("Spark Session Stopped ✓");
        }

        System.out.println("========================================");
        System.out.println("   RAILWAY DELAY INTELLIGENCE SYSTEM");
        System.out.println("   COMPLETED SUCCESSFULLY");
        System.out.println("========================================");
    }
}