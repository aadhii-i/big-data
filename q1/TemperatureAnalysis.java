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

        System.exit(job.waitForCompletion(true) ? 0 : 1);aadhi@aadhi-HP-EliteBook-840-G5:~/big data lab/q1$ echo "========================================"
echo "Name: Adhil Rahiman M"
echo "Roll No: 2023BCS0187"
echo "========================================"

hadoop jar temperature.jar TemperatureAnalysis \
/user/$USER/q1/input \
/user/$USER/q1/output

hdfs dfs -cat /user/$USER/q1/output/part-r-00000
========================================
Name: Adhil Rahiman M
Roll No: 2023BCS0187
========================================
JAR does not exist or is not a normal file: /home/aadhi/big data lab/q1/temperature.jar
2026-08-10 15:39:46,419 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
cat: Call From aadhi-HP-EliteBook-840-G5/127.0.1.1 to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused; For more details see:  http://wiki.apache.org/hadoop/ConnectionRefused
aadhi@aadhi-HP-EliteBook-840-G5:~/big data lab/q1$ 
    }
}