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

public class FlowCount {



    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1 将一行内容转成string
            String ling = value.toString();

            // 2 切分字段
            String[] fields = ling.split("\t");

            // 3 取出手机号码
            String phoneNum = fields[1];

            // 4 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            // 5 写出数据
            context.write(new Text(phoneNum), new FlowBean(upFlow, downFlow));
        }
    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context)
                throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;

            // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
            for (FlowBean bean : values) {
                sum_upFlow += bean.getUpFlow();
                sum_downFlow += bean.getDownFlow();
            }

            // 2 封装对象
            FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);
            context.write(key, resultBean);
        }
    }

    public static void main(String[] args) throws Exception {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCount.class);

        // 8 指定自定义数据分区
        job.setPartitionerClass(ProvincePartitioner.class);

        // 9 同时指定相应数量的reduce task
        job.setNumReduceTasks(5);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 5 指定job的输入原始文件所在目录
        Path inpath = new Path("hdfs://localhost:9000/mydir/input/phone_data.txt");
        Path outPath = new Path("hdfs://localhost:9000/mydir/output"
                + FlowCount.class.toString() + System.currentTimeMillis());
        FileInputFormat.setInputPaths(job, inpath);
        FileOutputFormat.setOutputPath(job, outPath);
        /*FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));*/

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
