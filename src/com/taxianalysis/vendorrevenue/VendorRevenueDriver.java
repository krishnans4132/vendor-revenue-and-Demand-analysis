package com.taxianalysis.vendorrevenue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver class - configures and launches the Vendor Revenue MapReduce job.
 *
 * USAGE (after packaging into VendorRevenueAnalysis.jar):
 *   hadoop jar VendorRevenueAnalysis.jar \
 *       com.taxianalysis.vendorrevenue.VendorRevenueDriver \
 *       <input path on HDFS> <output path on HDFS>
 *
 * Example:
 *   hadoop jar VendorRevenueAnalysis.jar \
 *       com.taxianalysis.vendorrevenue.VendorRevenueDriver \
 *       /user/yourname/vendor-revenue/input \
 *       /user/yourname/vendor-revenue/output
 */
public class VendorRevenueDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: VendorRevenueDriver <input path on HDFS> <output path on HDFS>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Vendor Revenue Analysis - NYC Yellow Taxi");

        // Tell Hadoop which JAR to ship to the cluster (this class's JAR)
        job.setJarByClass(VendorRevenueDriver.class);

        job.setMapperClass(VendorRevenueMapper.class);
        job.setCombinerClass(VendorRevenueReducer.class); // safe: SUM is associative/commutative
        job.setReducerClass(VendorRevenueReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true); // true = print progress to console
        System.exit(success ? 0 : 1);
    }
}
