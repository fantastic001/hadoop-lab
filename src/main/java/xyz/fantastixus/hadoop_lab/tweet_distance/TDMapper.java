package xyz.fantastixus.hadoop_lab.tweet_distance;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.B;

public class TDMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
    @Override
    protected void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        String b = value.toString();
        String a = context.getConfiguration().get("tweet");
        DLDistance distance = new DLDistance(a);
        context.write(new IntWritable(distance.get(b)), new Text(b));
    }
    
}
