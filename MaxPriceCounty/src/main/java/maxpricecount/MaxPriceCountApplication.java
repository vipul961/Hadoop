package maxpricecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class MaxPriceCountApplication {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);


        Configuration config = new Configuration();
        Job job = new Job(config, "Find sales by county");

        FileSystem fs = input.getFileSystem(config);
        // Deletes the output directory if it exists, don't have to delete manually in
        // hdfs
        fs.delete(output, true);

        job.setJarByClass(MaxPriceCountApplication.class);

        job.setMapperClass(MaxPriceCountApplicationMapper.class);
        job.setReducerClass(MaxPriceCountApplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

class MaxPriceCountApplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");
        try {
            double amount = Double.parseDouble(tokens[1].trim());
            context.write(new Text(tokens[8]), new DoubleWritable(amount));
        } catch (NumberFormatException ex) {
            // To handle the header value
        }
    }
}

class MaxPriceCountApplicationReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    TreeMap<BigDecimal, String> maxPriceCounty = new TreeMap<BigDecimal, String>(Collections.reverseOrder());

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        BigDecimal sum = new BigDecimal(0);
        for (DoubleWritable value : values) {
            sum = sum.add(new BigDecimal(value.get()));
        }
        maxPriceCounty.put(sum, key.toString());

    }

    @Override
    protected void cleanup(Reducer<Text, DoubleWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {
        for (Map.Entry<BigDecimal, String> entry : maxPriceCounty.entrySet()) {
            BigDecimal finalSalesAmount = entry.getKey().setScale(2, RoundingMode.HALF_EVEN);
            context.write(new Text(entry.getValue()), new Text(finalSalesAmount.toPlainString()));
        }
    }
}
