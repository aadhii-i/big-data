import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLength {

    public static class LengthMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final IntWritable length = new IntWritable();
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] words = line.split("\\s+");

            for (String word : words) {

                word = word.replaceAll("[^a-zA-Z0-9]", "");

                if (!word.isEmpty()) {
                    length.set(word.length());
                    context.write(length, one);
                }
            }
        }
    }

    public static class LengthReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Word Length Distribution");

        job.setJarByClass(WordLength.class);

        job.setMapperClass(LengthMapper.class);
        job.setReducerClass(LengthReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

