package xyz.fantastixus.hadoop_lab.word_index;

import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class WIMapper extends Mapper<LongWritable, Text, Text, WordIndexEntryWritable> { 
    protected void map(LongWritable key, Text value, 
        org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,WordIndexEntryWritable>.Context context) 
        throws java.io.IOException ,InterruptedException 
    {
        String line = value.toString();
        Path path = MapperUtils.getPath(context.getInputSplit());
        String[] words = line.split(" ");
        HashMap<String, WordIndexEntry> map = new HashMap<>();
        for (int i = 0; i<words.length; i++) {
            if (!words[i].equals("")) {
                if (!map.containsKey(words[i])) {
                    map.put(words[i], new WordIndexEntry(words[i], path.toString()));
                }
                map.get(words[i]).addPosition(i);
            }
        }
        for (String w : map.keySet()) {
            context.write(new Text(w), new WordIndexEntryWritable(map.get(w)));
        }

    };
    
}
