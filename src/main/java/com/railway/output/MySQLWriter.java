package com.railway.output;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class MySQLWriter {
    private static final String URL = "jdbc:mysql://localhost:3306/railway_db";
    private static final String USER = "prasanna";
    private static final String PASSWORD = "*****";

    private static Properties getProperties() {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        return props;
    }

    //writing Zone analysis into MySQL

    public static void writeZoneAnalysis(Dataset<Row> zoneSummary) {
        System.out.println("Writing Zone Analysis to MySQL...");

        zoneSummary.write()
                .mode(SaveMode.Overwrite)
                .jdbc(URL, "zone_analysis", getProperties());

        System.out.println("Zone Analysis written to MySQL ✓");
    }

    //stations into MySQL
    public static void writeStationAnalysis(Dataset<Row> stationSummary) {
        System.out.println("Writing Station Analysis to MySQL...");

        stationSummary.write()
                .mode(SaveMode.Overwrite)
                .jdbc(URL, "station_analysis", getProperties());

        System.out.println("Station Analysis written to MySQL ✓");
    }

    //Route Analysis to MySQL
    public static void writeRouteAnalysis(Dataset<Row> routeSummary) {
        System.out.println("Writing Route Analysis to MySQL...");

        routeSummary.write()
                .mode(SaveMode.Overwrite)
                .jdbc(URL, "route_analysis", getProperties());

        System.out.println("Route Analysis written to MySQL ✓");
    }

    //Duration analysis into MySQL
    public static void writeDurationAnalysis(Dataset<Row> durationSummary) {
        System.out.println("Writing Duration Analysis to MySQL...");

        durationSummary
                .select(
                        "train_number",
                        "train_name",
                        "duration_hours",
                        "duration_mins",
                        "total_minutes",
                        "journey_class",
                        "zone"
                )
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(URL, "duration_analysis", getProperties());

        System.out.println("Duration Analysis written to MySQL ✓");
    }



}
