import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @Auther: Kylinrix
 * @Date: 2018/10/11 13:57
 * @Email: Kylinrix@outlook.com
 * @Description:使用key与value的反转，实现按照词频排序
 */
public class SortDemo {

    public static final Log LOG = LogFactory.getLog(FileInputFormat.class);

    public static class SortMapper extends Mapper<LongWritable, Text,LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            StringTokenizer itr = new StringTokenizer(value.toString());

            Text word = new Text(itr.nextToken());
            LongWritable count = new LongWritable(Integer.parseInt(itr.nextToken()));

            context.write(count,word);
        }
    }

    public static class SortReducer extends Reducer<LongWritable,Text, LongWritable,Text> {
        private static int num = 0;
        @Override
        protected void reduce(LongWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException{
            Iterator<Text> itr = value.iterator();
            while(itr.hasNext()) {
                context.write(key, itr.next());
            }

        }
    }

    //实现倒序输出
    private static class LongWritableDecreasingComparator extends LongWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


    public  void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SortDemo.class);


        //job.setMapperClass(InverseMapper.class);

        //倒序
        job.setSortComparatorClass(LongWritableDecreasingComparator.class);


        //Mapper
        job.setMapperClass(SortDemo.SortMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //Reducer
        job.setReducerClass(SortDemo.SortReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output2/"));

        job.waitForCompletion(true);
        System.out.println("Finished");

    }
}
