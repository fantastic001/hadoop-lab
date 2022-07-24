package xyz.fantastixus.hadoop_lab.word_index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordIndexEntryWritable implements WritableComparable<WordIndexEntryWritable> {
    private WordIndexEntry entry; 
    public WordIndexEntryWritable() {
        this.entry = null;
    }

    public WordIndexEntryWritable(WordIndexEntry entry) {
        this.entry = entry;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(entry.getWord()).write(out);
        new Text(entry.getFilename()).write(out);
        ArrayWritable aw = new ArrayWritable(IntWritable.class);
        Writable[] arr = new Writable[entry.getPositions().size()];
        for (int i = 0; i < entry.getPositions().size(); i++) {
            arr[i] = new IntWritable(entry.getPositions().get(i));
        }
        new ArrayWritable(IntWritable.class, arr).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Text word = new Text();
        Text filename = new Text();
        ArrayWritable positions = new ArrayWritable(IntWritable.class);
        word.readFields(in);
        filename.readFields(in);
        positions.readFields(in);
        entry = new WordIndexEntry(word.toString(), filename.toString());
        for (Writable pos : positions.get()) {
            entry.addPosition(((IntWritable) pos).get());;
        }
    }

    @Override
    public int compareTo(WordIndexEntryWritable o) {
        return get().compareTo(o.get());
    }

    public WordIndexEntry get() {
        return entry;
    }

    @Override
    public String toString() {
        return entry.toString();
    }
    
}
