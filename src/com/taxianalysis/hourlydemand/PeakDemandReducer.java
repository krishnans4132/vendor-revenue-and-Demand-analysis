package com.taxianalysis.hourlydemand;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PeakDemandReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    public void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

        int totalTrips = 0;

        for (IntWritable value : values) {
            totalTrips += value.get();
        }

        result.set(totalTrips);
        context.write(key, result);
    }
}