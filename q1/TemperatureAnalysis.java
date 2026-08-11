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

public class TemperatureAnalysis {

    public static class TemperatureMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text year = new Text();
        private IntWritable temperature = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.startsWith("Year")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length != 4) {
                return;
            }

            year.set(fields[0].trim());

            try {
                temperature.set(Integer.parseInt(fields[3].trim()));
                context.write(year, temperature);
            } catch (NumberFormatException e) {
                // Ignore invalid records
            }
        }
    }

    public static class TemperatureReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values,
                            Context context)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;

            for (IntWritable value : values) {
                int temp = value.get();

                if (temp > max)
                    max = temp;

                if (temp < min)
                    min = temp;
            }

            context.write(
                key,
                new Text("Max: " + max + " Min: " + min)
            );
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Temperature Analysis");

        job.setJarByClass(TemperatureAnalysis.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}