package xyz.fantastixus.hadoop_lab.wordfreq;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordFreqMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        TreeMap<String, Integer> dict = new TreeMap<>();
        String[] words = line.split(" ");
        for (String word : words) {
            if (! word.isEmpty()) {
                if (! dict.containsKey(word)) {
                    dict.put(word, 1);
                }
                else {
                    dict.put(word, dict.get(word)+1);
                }
            }
        }
        dict.forEach((String word, Integer freq) -> {
            try {
                context.write(new Text(word), new IntWritable(freq));
            } catch (IOException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }
    
}
