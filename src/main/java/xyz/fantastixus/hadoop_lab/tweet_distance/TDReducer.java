package xyz.fantastixus.hadoop_lab.tweet_distance;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class TDReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable>{

    @Override
    protected void reduce(IntWritable arg0, Iterable<Text> arg1,
            Reducer<IntWritable, Text, IntWritable, IntWritable>.Context arg2)
            throws IOException, InterruptedException {
        int count = 0;
        for (Text w : arg1) {
            count++;
        }
        arg2.write(arg0, new IntWritable(count));
    }
    
}
