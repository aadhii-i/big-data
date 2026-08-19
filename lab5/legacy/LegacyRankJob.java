package com.college;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LegacyRankJob {

    public static class RankMapper
        extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private final IntWritable outKey = new IntWritable();
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty())
                return;

            String[] parts = line.split(",");

            if (parts.length != 2)
                return;

            int rollNo = Integer.parseInt(parts[0].trim());
            int marks = Integer.parseInt(parts[1].trim());

            outKey.set(Integer.MAX_VALUE - marks);
            outValue.set(rollNo);

            context.write(outKey, outValue);
        }
    }

    public static class RankReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private int rank = 0;

        @Override
        protected void reduce(
                IntWritable key,
                Iterable<IntWritable> rollNumbers,
                Context context)
                throws IOException, InterruptedException {

            for (IntWritable rollNo : rollNumbers) {
                rank++;
                context.write(rollNo, new IntWritable(rank));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: LegacyRankJob <input dir> <output dir>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Legacy Rank Job");

        job.setJarByClass(LegacyRankJob.class);

        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(
            job, new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job, new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
