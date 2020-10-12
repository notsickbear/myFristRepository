package mapreduce_ex_01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);//主类

        job.setMapperClass(TokenizerMapper.class);//mapper
        job.setCombinerClass(IntSumReducer.class);//作业合成类
        job.setReducerClass(IntSumReducer.class);//reducer

        job.setOutputKeyClass(Text.class);//设置作业输出数据的关键类
        job.setOutputValueClass(IntWritable.class);//设置作业输出值类
        String inputfile = "hdfs://localhost:9000/mydir/input/WordCount.java";
        FileInputFormat.addInputPath(job, new Path(inputfile));
        String outputfile = "hdfs://localhost:9000/mydir/output";
        FileOutputFormat.setOutputPath(job, new Path(outputfile));
        System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
    }

    /*Mapper<Object, Text, Text, IntWritable>
    Mapper< Text, Text, Text, Text>
    Mapper< Text, IntWritable, Text, IntWritable>
    第一二个表示输入map的key和value，从InputFormat传过来的，key默认是字符偏移量，
    value默认是一行,第三四个表示输出的key和value
    mapper中的方法map(Object key, Text value, Context context)
    key和value表示输入的key和value，处理后的数据写入context，
    使用方法context.write(key, value);，这里的key和value会传递给下一个过程
    * TokenizerMapper 继续自 Mapper<Object, Text, Text, IntWritable>
   *
   * [一个文件就一个map,两个文件就会有两个map]
   * map[这里读入输入文件内容 以" \t\n\r\f" 进行分割，然后设置 word ==> one 的key/value对]
   * Writable的主要特点是它使得Hadoop框架知道对一个Writable类型的对象怎样进行serialize以及deserialize.
   * WritableComparable在Writable的基础上增加了compareT接口，使得Hadoop框架知道怎样对WritableComparable类型的对象进行排序。
    */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(line.toString());
            while (itr.hasMoreTokens()) {
                String wordContent = itr.nextToken();
                word.set(wordContent);
                context.write(word, one);
            }
        }
    }

    /*
     * IntSumReducer 继承自 Reducer<Text,IntWritable,Text,IntWritable>
     * [不管几个Map,都只有一个Reduce,这是一个汇总]
     * reduce[循环所有的map值,把word ==> one 的key/value对进行汇总]
     * 这里的key为Mapper设置的word[每一个key/value都会有一次reduce]
     * 当循环结束后，最后的确context就是最后的结果.
     *
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
                //System.out.println(val.get());
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}