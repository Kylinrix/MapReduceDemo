import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * @Auther: Kylinrix
 * @Date: 2018/10/11 15:20
 * @Email: Kylinrix@outlook.com
 * @Description: 使用TreeSet实现TopN
 */



class DataBean{
    String word;
    Long count;
}

public class TopNDemo {
    public static final Log LOG =
            LogFactory.getLog(FileInputFormat.class);


    public static class TopNMapper extends Mapper<LongWritable, Text, LongWritable,Text> {
        TreeSet<DataBean> cachedTopN = null;
        Integer N = 10;

        @Override
        protected void setup(Context context){
            cachedTopN = new TreeSet<DataBean>(new Comparator<DataBean>() {
                @Override
                public int compare(DataBean o1, DataBean o2) {
                    int ret = 0;
                    if (o1.count > o2.count) {
                        ret = -1;
                    } else if (o1.count < o2.count) {
                        ret = 1;
                    }
                    return ret;
                }
            });

        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());

            //Spark 111
            DataBean dataBean = new DataBean();
            dataBean.word = itr.nextToken();
            dataBean.count = Long.parseLong(itr.nextToken());

            cachedTopN.add(dataBean);
            if(cachedTopN.size()>N){
                cachedTopN.pollLast();
            }
        }

        @Override
        protected  void cleanup(Context context) throws IOException, InterruptedException {
            for (DataBean d : cachedTopN) {
                context.write(new LongWritable(d.count),new Text(d.word));
            }
        }
    }


    public static class TopNReducer extends Reducer<LongWritable, Text,LongWritable, Text> {

        TreeSet<DataBean> cachedTopN = null;
        Integer N = 10;
        @Override
        protected void setup(Context context){
            cachedTopN = new TreeSet<DataBean>(new Comparator<DataBean>() {
                @Override
                public int compare(DataBean o1, DataBean o2) {
                    int ret = 0;
                    if (o1.count > o2.count) {
                        ret = -1;
                    } else if (o1.count < o2.count) {
                        ret = 1;
                    }
                    return ret;
                }
            });

        }
        @Override
        protected void reduce(LongWritable key,
                              Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {


            //Spark 111
            DataBean dataBean = new DataBean();
            dataBean.word = values.iterator().next().toString();
            dataBean.count = key.get();

            cachedTopN.add(dataBean);
            if(cachedTopN.size()>N){
                cachedTopN.pollLast();
            }

        }

        @Override
        protected  void cleanup(Context context) throws IOException, InterruptedException {
            for (DataBean d : cachedTopN) {
                context.write(new LongWritable(d.count),new Text(d.word));
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TopNDemo.class);


        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("/Users/lky/IdeaProjects/SparkDemo/output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("outputTopN/"));

        job.waitForCompletion(true);
        System.out.println("Finished");

    }
}
