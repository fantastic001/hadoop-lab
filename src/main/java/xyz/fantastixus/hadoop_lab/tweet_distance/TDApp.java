package xyz.fantastixus.hadoop_lab.tweet_distance;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TDApp {

    public static void main( String[] args )
    {
        String inputPath = args[0];
        String outputPath = args[1];

        Job job;
        try {
            Configuration conf = new Configuration();
            conf.set("tweet", args[2]);
            job = new Job(conf);
        } catch (IOException e1) {
            System.exit(255);
            return;
        }
        job.setJarByClass(TDApp.class);
        job.setJobName("Tweet distance");

        try {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(TDMapper.class);
        job.setReducerClass(TDReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }    
}

