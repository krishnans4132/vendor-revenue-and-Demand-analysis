package com.taxianalysis.vendorrevenue;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Vendor Revenue Analysis.
 * This class is also reused as the Combiner (see Driver) because
 * summation is associative and commutative - a combiner can safely
 * pre-aggregate values on the Mapper's node before they are shuffled
 * across the network to the Reducer.
 *
 * INPUT  : (VendorID, [amount1, amount2, amount3, ...]) - every revenue
 *          value emitted for one VendorID, already grouped together by
 *          the shuffle-and-sort phase.
 *
 * OUTPUT : (VendorID, totalRevenue) - one line per vendor.
 */
public class VendorRevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }

        // Round to 2 decimal places since this represents currency
        sum = Math.round(sum * 100.0) / 100.0;

        result.set(sum);
        context.write(key, result);
    }
}
