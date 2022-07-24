package xyz.fantastixus.hadoop_lab.word_index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WIReducer extends Reducer<Text, WordIndexEntryWritable, Text, WordIndexWritable> {
    @Override
    protected void reduce(Text arg0, Iterable<WordIndexEntryWritable> arg1,
            Reducer<Text, WordIndexEntryWritable, Text, WordIndexWritable>.Context arg2)
            throws IOException, InterruptedException {
        String word = arg0.toString();
        WordIndexWritable index = new WordIndexWritable();
        for (WordIndexEntryWritable entity : arg1) {
            index.add(entity);
        }
        arg2.write(new Text(word), index);
    }
}
