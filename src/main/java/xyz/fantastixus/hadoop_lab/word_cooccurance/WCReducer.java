package xyz.fantastixus.hadoop_lab.word_cooccurance;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, WordNodeWritable, Text, LinkWritable> {
    
    @Override
    protected void reduce(Text arg0, Iterable<WordNodeWritable> arg1,
            Reducer<Text, WordNodeWritable, Text, LinkWritable>.Context arg2) throws IOException, InterruptedException {
        String word = arg0.toString();
        WordNode node = new WordNode(word);
        for (WordNodeWritable iter : arg1) {
            node.merge(iter.get());
        }
        int count = node.getCount();
        for (String n : node.getNeighbours().keySet()) {
            int frequency = node.getNeighbours().get(n);
            arg2.write(new Text(word), new LinkWritable(word, n, frequency * 1.0 / count));
        }
    }
    
}
