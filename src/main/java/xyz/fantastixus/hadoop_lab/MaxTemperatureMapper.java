package xyz.fantastixus.hadoop_lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15,19);
        int airTemp; 
        if (line.charAt(87) == '+') {
            airTemp = Integer.parseInt(line.substring(88, 92));
        }
        else {
            airTemp = Integer.parseInt(line.substring(87, 92));
        }
        context.write(new Text(year), new IntWritable(airTemp));
    }
    
}
