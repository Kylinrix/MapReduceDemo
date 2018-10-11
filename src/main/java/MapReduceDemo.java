import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @Auther: lky
 * @param:
 * @return:
 * @Date: 2018/10/10
 * @Email: Kylinrix@outlook.com
 * @Description:
 */
public class MapReduceDemo {

    public static final Log LOG =
            LogFactory.getLog(FileInputFormat.class);   //定义log变量

    public static int[] max_3 = {0,0,0};
    /*
    * * 继承Mapper类需要定义四个输出、输出类型泛型：
    * * 四个泛型类型分别代表：
    * * KeyIn        (系统设置) Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）（系统默认为每行文字的起始位置）
    * * ValueIn      (系统设置) Mapper的输入数据的Value，这里是每行文字（系统默认为每行文字）
    * * KeyOut       (自行设置) Mapper的输出数据的Key，对数据进行提取，这里是每行文字中的单词"hello"
    * * ValueOut     (自行设置) Mapper的输出数据的Value，对数据进行处理的阶段，这里是每行文字中的出现的次数
    * * Writable接口是一个实现了序列化协议的序列化对象。
    * * 在Hadoop中定义一个结构化对象都要实现Writable接口，使得该结构化对象可以序列化为字节流，字节流也可以反序列化为结构化对象。
    * * LongWritable类型:Hadoop.io对Long类型的封装类型
    * */

    public static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        // 重写Map方法
        @Override
        //Mapper<LongWritable, Text, Text, LongWritable>.Context
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            /*
             // 获得每行文档内容，并且进行折分
            String[] words = value.toString().split(" |;|,|:|\\.");
            // 遍历折份的内容
            System.out.println(key);
            key.get();
            */
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                //这里的new Text(word)表示出现的单词，这里的新建的单词如果有重复，是在Shffule阶段再将这些1组成列表，再交给Reduce统一处理。
                context.write(new Text(itr.nextToken()), new LongWritable(1));
            }
        }
    }

    /*	 * 继承Reducer类需要定义四个输出、输出类型泛型：
    * * 四个泛型类型分别代表：
    * * KeyIn        (mapper的输出key) Reducer的输入数据的Key，这里是每行文字中的单词"hello"
    * * ValueIn      (Mapper的输出value) Reducer的输入数据的Value，这里是每行文字中的次数
    * * KeyOut       (自行设置) Reducer的输出数据的Key，这里是每行文字中的单词"hello"
    * * ValueOut     (自行设置) Reducer的输出数据的Value，这里是每行文字中的出现的总次数
    * */

    public static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key,
                              Iterable<LongWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable i : values) {
                sum += i.get();
            }
            // 输出总计结果
            context.write( key,new LongWritable(sum));

        }
    }



    public   void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 创建job对象
        Job job = Job.getInstance(new Configuration());
        // 指定程序的入口
        job.setJarByClass(MapReduceDemo.class);

        // 指定自定义的Mapper阶段的任务处理类
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        // 指定自定义的Reducer阶段的任务处理类
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //定义输入输出
        FileInputFormat.addInputPath(job, new Path("/input/Test"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        // 执行提交job方法，直到完成，参数true打印进度和详情
         job.waitForCompletion(true);
         System.out.println("Finished");

    }


}
