import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalaryAnalysis {

    public static class SalaryMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text department = new Text();
        private DoubleWritable salary = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.startsWith("EmpID")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length != 4) {
                return;
            }

            try {
                department.set(fields[2].trim());
                salary.set(Double.parseDouble(fields[3].trim()));

                context.write(department, salary);

            } catch (NumberFormatException e) {
                // Ignore invalid records
            }
        }
    }

    public static class SalaryReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {

                double salary = value.get();

                if (salary > max) {
                    max = salary;
                }

                if (salary < min) {
                    min = salary;
                }

                sum += salary;
                count++;
            }

            double average = sum / count;

            String result = String.format(
                "Maximum: %.2f Minimum: %.2f Average: %.2f",
                max, min, average
            );

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Department Salary Analysis"
        );

        job.setJarByClass(SalaryAnalysis.class);

        job.setMapperClass(SalaryMapper.class);
        job.setReducerClass(SalaryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
