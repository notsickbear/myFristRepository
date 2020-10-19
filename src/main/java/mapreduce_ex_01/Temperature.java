package mapreduce_ex_01;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class Temperature {
    /**
     * 四个泛型类型分别代表：
     * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
     * ValueIn      Mapper的输入数据的Value，这里是每行文字
     * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
     * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String year = line.substring(0, 4);
            int temperature = Integer.parseInt(line.substring(8));
            output.collect(new Text(year), new IntWritable(temperature));
        }
    }
    /**
     * 四个泛型类型分别代表：
     * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
     * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
     * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
     * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int maxValue = Integer.MIN_VALUE;
            //取values的最大值
            while (values.hasNext()) {
                int value = values.next().get();
                maxValue = Math.max(maxValue, value);
            }
            output.collect(key, new IntWritable(maxValue));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(Temperature.class);

        jobConf.setMapperClass(Temperature.Map.class);//mapper
        jobConf.setCombinerClass(Temperature.Reduce.class);//作业合成类
        jobConf.setReducerClass(Temperature.Reduce.class);//reducer

        jobConf.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
        jobConf.setOutputValueClass(IntWritable.class);//设置作业输出值类

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        String infile = "hdfs://localhost:9000/mydir/input/mapred3.txt";
        FileInputFormat.addInputPath(jobConf, new Path(infile));
        String outfile = "hdfs://localhost:9000/mydir/output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(jobConf, new Path(outfile));
        /*FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));*/

        JobClient.runJob(jobConf);
    }
}
