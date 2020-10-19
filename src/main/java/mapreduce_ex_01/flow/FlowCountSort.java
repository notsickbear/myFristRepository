package mapreduce_ex_01.flow;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountSort {
    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
        FlowBean bean = new FlowBean();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 1 拿到的是上一个统计程序输出的结果，已经是各手机号的总流量信息
            String line = value.toString();

            // 2 截取字符串并获取电话号、上行流量、下行流量
            String[] fields = line.split("\t| +");
            String phoneNbr = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);

            // 3 封装对象
            bean.set(upFlow, downFlow);
            v.set(phoneNbr);

            // 4 输出
            context.write(bean, v);
        }
    }

    static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{

        @Override
        protected void reduce(FlowBean bean, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(values.iterator().next(), bean);
        }
    }

    public static void main(String[] args) throws Exception {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountSort.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 5 指定job的输入原始文件所在目录
        /*FileInputFormat.setInputPaths(job, new Path(args[0]));*/
        Path inpath = new Path("hdfs://localhost:9000/mydir/input/phone_data_count.txt");
        FileInputFormat.setInputPaths(job, inpath);

        /*Path outPath = new Path(args[1]);*/
//		FileSystem fs = FileSystem.get(configuration);
//		if (fs.exists(outPath)) {
//			fs.delete(outPath, true);
//		}
        Path outPath = new Path("hdfs://localhost:9000/mydir/output"
                + FlowCountSort.class.toString() + System.currentTimeMillis());
        FileOutputFormat.setOutputPath(job, outPath);

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
