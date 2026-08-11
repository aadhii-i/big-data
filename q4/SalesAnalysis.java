import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesAnalysis {

    // JOB 1: Calculate total revenue for each product

    public static class RevenueMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text product = new Text();
        private DoubleWritable revenue = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.startsWith("TransactionID")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length != 4) {
                return;
            }

            try {
                String productName = fields[1].trim();

                double quantity =
                        Double.parseDouble(fields[2].trim());

                double price =
                        Double.parseDouble(fields[3].trim());

                double totalRevenue = quantity * price;

                product.set(productName);
                revenue.set(totalRevenue);

                context.write(product, revenue);

            } catch (NumberFormatException e) {
                // Ignore invalid records
            }
        }
    }


    public static class RevenueReducer
            extends Reducer<Text, DoubleWritable,
                            Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double total = 0;

            for (DoubleWritable value : values) {
                total += value.get();
            }

            context.write(
                key,
                new DoubleWritable(total)
            );
        }
    }


    // JOB 2: Find Top 3 products

    public static class TopMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text product = new Text();
        private DoubleWritable revenue = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\t");

            if (parts.length != 2) {
                return;
            }

            product.set(parts[0]);
            revenue.set(Double.parseDouble(parts[1]));

            context.write(product, revenue);
        }
    }


    public static class TopReducer
            extends Reducer<Text, DoubleWritable,
                            Text, DoubleWritable> {

        private PriorityQueue<ProductRevenue> top3;

        @Override
        protected void setup(Context context) {

            top3 = new PriorityQueue<>(
                3,
                (a, b) -> Double.compare(
                    a.revenue,
                    b.revenue
                )
            );
        }

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context) {

            double revenue = 0;

            for (DoubleWritable value : values) {
                revenue = value.get();
            }

            ProductRevenue item =
                new ProductRevenue(
                    key.toString(),
                    revenue
                );

            top3.add(item);

            if (top3.size() > 3) {
                top3.poll();
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            ProductRevenue[] result =
                top3.toArray(
                    new ProductRevenue[0]
                );

            Arrays.sort(
                result,
                (a, b) -> Double.compare(
                    b.revenue,
                    a.revenue
                )
            );

            for (ProductRevenue item : result) {

                context.write(
                    new Text(item.product),
                    new DoubleWritable(item.revenue)
                );
            }
        }
    }


    static class ProductRevenue {

        String product;
        double revenue;

        ProductRevenue(
                String product,
                double revenue) {

            this.product = product;
            this.revenue = revenue;
        }
    }


    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        // =========================
        // JOB 1
        // =========================

        Job job1 =
                Job.getInstance(
                    conf,
                    "Product Revenue"
                );

        job1.setJarByClass(
                SalesAnalysis.class
        );

        job1.setMapperClass(
                RevenueMapper.class
        );

        job1.setReducerClass(
                RevenueReducer.class
        );

        job1.setOutputKeyClass(
                Text.class
        );

        job1.setOutputValueClass(
                DoubleWritable.class
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


        // =========================
        // JOB 2
        // =========================

        Job job2 =
                Job.getInstance(
                    conf,
                    "Top 3 Products"
                );

        job2.setJarByClass(
                SalesAnalysis.class
        );

        job2.setMapperClass(
                TopMapper.class
        );

        job2.setReducerClass(
                TopReducer.class
        );

        job2.setMapOutputKeyClass(
                Text.class
        );

        job2.setMapOutputValueClass(
                DoubleWritable.class
        );

        job2.setOutputKeyClass(
                Text.class
        );

        job2.setOutputValueClass(
                DoubleWritable.class
        );

        // One reducer for global Top 3
        job2.setNumReduceTasks(1);

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

