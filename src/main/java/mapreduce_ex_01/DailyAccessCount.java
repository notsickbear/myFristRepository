package mapreduce_ex_01;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class DailyAccessCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] array = line.split(",");
            Text keyOutput = new Text(array[1]);
            output.collect(keyOutput, one);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(DailyAccessCount.class);

        jobConf.setMapperClass(DailyAccessCount.Map.class);//mapper
        jobConf.setCombinerClass(DailyAccessCount.Reduce.class);//作业合成类
        jobConf.setReducerClass(DailyAccessCount.Reduce.class);//reducer

        jobConf.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
        jobConf.setOutputValueClass(IntWritable.class);//设置作业输出值类

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        String infile = "hdfs://localhost:9000/mydir/input/mapred2.txt";
        FileInputFormat.addInputPath(jobConf, new Path(infile));
        String outfile = "hdfs://localhost:9000/mydir/output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(jobConf, new Path(outfile));
        /*FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));*/

        JobClient.runJob(jobConf);
    }
}
