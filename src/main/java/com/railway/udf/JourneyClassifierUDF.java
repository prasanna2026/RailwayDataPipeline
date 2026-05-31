package com.railway.udf;

import org.apache.spark.sql.api.java.UDF1;

public class JourneyClassifierUDF implements UDF1<Long,String>{

    @Override
    public String call(Long totalMinutes) throws Exception {
        if(totalMinutes == null){
            return "UNKNOWN";
        }

        if(totalMinutes <= 60){
            return "SHORT";
        }else if(totalMinutes <= 180){
            return "MEDIUM";
        }else if(totalMinutes <= 320){
            return "LONG";
        }else if(totalMinutes <= 720){
            return "EXTRA LONG";
        }else{
            return "ULTRA";
        }
    }

}
