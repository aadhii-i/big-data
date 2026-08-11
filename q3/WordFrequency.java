import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordFrequency {

    // JOB 1: Word Count + Filtering

    public static class WordMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE =
                new IntWritable(1);

        private Text wordText = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString()
                    .toLowerCase()
                    .replaceAll("[^a-z0-9\\s]", " ");

            StringTokenizer tokenizer =
                    new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {

                String word = tokenizer.nextToken();

                wordText.set(word);

                context.write(wordText, ONE);
            }
        }
    }

    public static class WordReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            // Filter: only frequencies greater than 5
            if (sum > 5) {
                context.write(key, new IntWritable(sum));
            }
        }
    }


    // JOB 2: Sort by Frequency Descending

    public static class SortMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable frequency = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\t");

            if (parts.length != 2) {
                return;
            }

            word.set(parts[0]);
            frequency.set(Integer.parseInt(parts[1]));

            context.write(frequency, word);
        }
    }

    public static class SortReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            for (Text word : values) {
                context.write(key, word);
            }
        }
    }


    // Descending integer comparator

    public static class DescendingComparator
            extends IntWritable.Comparator {

        public DescendingComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            return -super.compare(
                    b1, s1, l1,
                    b2, s2, l2
            );
        }
    }


    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        // ==========================
        // JOB 1
        // ==========================

        Job job1 =
                Job.getInstance(
                        conf,
                        "Word Frequency"
                );

        job1.setJarByClass(
                WordFrequency.class
        );

        job1.setMapperClass(
                WordMapper.class
        );

        job1.setReducerClass(
                WordReducer.class
        );

        job1.setOutputKeyClass(
                Text.class
        );

        job1.setOutputValueClass(
                IntWritable.class
        );

        FileInputFormat.addInputPath(
                job1,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job1,
                new Path(args[1])
        );

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }


        // ==========================
        // JOB 2
        // ==========================

        Job job2 =
                Job.getInstance(
                        conf,
                        "Descending Word Frequency"
                );

        job2.setJarByClass(
                WordFrequency.class
        );

        job2.setMapperClass(
                SortMapper.class
        );

        job2.setReducerClass(
                SortReducer.class
        );

        job2.setMapOutputKeyClass(
                IntWritable.class
        );

        job2.setMapOutputValueClass(
                Text.class
        );

        job2.setOutputKeyClass(
                IntWritable.class
        );

        job2.setOutputValueClass(
                Text.class
        );

        // One reducer ensures global ordering
        job2.setNumReduceTasks(1);

        job2.getConfiguration().set(
                "mapreduce.job.output.key.comparator.class",
                DescendingComparator.class.getName()
        );

        FileInputFormat.addInputPath(
                job2,
                new Path(args[1])
        );

        FileOutputFormat.setOutputPath(
                job2,
                new Path(args[2])
        );

        System.exit(
                job2.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}
