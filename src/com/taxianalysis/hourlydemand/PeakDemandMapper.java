package com.taxianalysis.hourlydemand;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PeakDemandMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private final Text hourKey = new Text();

    @Override
    public void map(
            LongWritable key,
            Text value,
            Context context) throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip header and empty lines
        if (line.isEmpty() || line.equals("pickup_hour")) {
            return;
        }

        try {
            int hour = Integer.parseInt(line);

            // Valid pickup hours are 0-23
            if (hour >= 0 && hour <= 23) {
                hourKey.set(String.valueOf(hour));
                context.write(hourKey, ONE);
            }
        } catch (NumberFormatException e) {
            // Skip malformed records
        }
    }
}