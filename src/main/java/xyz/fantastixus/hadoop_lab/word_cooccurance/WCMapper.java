package xyz.fantastixus.hadoop_lab.word_cooccurance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WCMapper extends Mapper<LongWritable, Text, Text, WordNodeWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WordNodeWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineSplit = line.split(" ");
        ArrayList<String> words = new ArrayList<>();
        for (String w : lineSplit) {
            if (! w.equals("")) words.add(w);
        }
        HashMap<String, WordNode> m = new HashMap<>();
        for (int i = 0; i<words.size() - 1; i++) {
            String word = words.get(i);
            String nextWord = words.get(i+1);
            if (!m.containsKey(word)) {
                m.put(word, new WordNode(word));
            }
            m.get(word).addLink(nextWord);
        }
        for (String w : m.keySet()) {
            context.write(new Text(w), new WordNodeWritable(m.get(w)));
        }
    }
}
