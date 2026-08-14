package com.taxianalysis.vendorrevenue;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for Vendor Revenue Analysis on the NYC Yellow Taxi dataset.
 *
 * INPUT  : one line of the pre-processed CSV file, e.g.
 *              VendorID,total_amount
 *              1,15.30
 *              2,9.80
 *
 * OUTPUT : (VendorID, total_amount) key-value pairs, e.g.
 *              <Text "1">, <DoubleWritable 15.30>
 *
 * The Mapper does three things:
 *   1. Skips the header row and any blank lines.
 *   2. Parses the two CSV fields (VendorID, total_amount).
 *   3. Discards records that are malformed, non-numeric, or have a
 *      non-positive revenue value (refunds / data errors), so bad
 *      data never reaches the Reducer.
 *
 * Hadoop Counters are used instead of print statements so you can see
 * exactly how many records were valid/skipped after the job finishes
 * (visible in the job's console summary and the ResourceManager UI) -
 * this is useful evidence for your report and viva.
 */
public class VendorRevenueMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text vendorKey = new Text();
    private final DoubleWritable revenueValue = new DoubleWritable();

    public enum RecordCounters {
        VALID_RECORDS,
        SKIPPED_HEADER,
        SKIPPED_MALFORMED,
        SKIPPED_NON_POSITIVE_AMOUNT
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // 1. Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // 2. Skip the header row
        if (line.startsWith("VendorID")) {
            context.getCounter(RecordCounters.SKIPPED_HEADER).increment(1);
            return;
        }

        // 3. Split into exactly two fields
        String[] fields = line.split(",");
        if (fields.length != 2) {
            context.getCounter(RecordCounters.SKIPPED_MALFORMED).increment(1);
            return;
        }

        String vendorId = fields[0].trim();
        String amountStr = fields[1].trim();

        if (vendorId.isEmpty() || amountStr.isEmpty()) {
            context.getCounter(RecordCounters.SKIPPED_MALFORMED).increment(1);
            return;
        }

        try {
            double amount = Double.parseDouble(amountStr);

            // Refunds, cancellations, or corrupted rows can produce
            // zero/negative total_amount - these should not be counted
            // as revenue.
            if (amount <= 0.0) {
                context.getCounter(RecordCounters.SKIPPED_NON_POSITIVE_AMOUNT).increment(1);
                return;
            }

            vendorKey.set(vendorId);
            revenueValue.set(amount);
            context.write(vendorKey, revenueValue);
            context.getCounter(RecordCounters.VALID_RECORDS).increment(1);

        } catch (NumberFormatException e) {
            // total_amount was not a valid number - skip this record
            context.getCounter(RecordCounters.SKIPPED_MALFORMED).increment(1);
        }
    }
}
