package xyz.fantastixus.hadoop_lab.wordfreq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordFreqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text arg0, Iterable<IntWritable> arg1,
            Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
        String word = arg0.toString();
        int sum = 0;
        for (IntWritable f : arg1) {
            sum += f.get();
        }
        arg2.write(new Text(word), new IntWritable(sum));
    }
}
