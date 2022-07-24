package xyz.fantastixus.hadoop_lab.word_index;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WIApp {

    public static void main( String[] args )
    {
        String inputPath = args[0];
        String outputPath = args[1];

        Job job;
        try {
            job = new Job();
        } catch (IOException e1) {
            System.exit(255);
            return;
        }
        job.setJarByClass(WIApp.class);
        job.setJobName("Word index");

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

        job.setMapperClass(WIMapper.class);
        job.setReducerClass(WIReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WordIndexEntryWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordIndexWritable.class);

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

