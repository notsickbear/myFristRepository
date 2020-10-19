package mapreduce_ex_01;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class RemoveRepeat {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private final static Text placeholder = new Text("");
        private final Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, placeholder);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
        private static int sum = 0;
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                values.next();
                output.collect(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(RemoveRepeat.class);

        jobConf.setMapperClass(RemoveRepeat.Map.class);//mapper
        jobConf.setCombinerClass(RemoveRepeat.Reduce.class);//作业合成类
        jobConf.setReducerClass(RemoveRepeat.Reduce.class);//reducer

        jobConf.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
        jobConf.setOutputValueClass(IntWritable.class);//设置作业输出值类

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        String infile = "hdfs://localhost:9000/mydir/input/WordCount.java";
        FileInputFormat.addInputPath(jobConf, new Path(infile));
        String outfile = "hdfs://localhost:9000/mydir/output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(jobConf, new Path(outfile));
        /*FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));*/

        JobClient.runJob(jobConf);
    }
}
