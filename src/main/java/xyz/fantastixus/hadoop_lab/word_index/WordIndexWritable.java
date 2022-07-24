package xyz.fantastixus.hadoop_lab.word_index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import xyz.fantastixus.hadoop_lab.word_cooccurance.WordNodeWritable;

public class WordIndexWritable implements WritableComparable<WordIndexWritable> {

    public ArrayList<WordIndexEntryWritable> entities;

    public WordIndexWritable() {
        entities = new ArrayList<>();
    }

    public void add(WordIndexEntryWritable entry) {
        entities.add(entry);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        Writable[] arr = new Writable[entities.size()];
        for (int i = 0; i<entities.size(); i++) {
            arr[i] = entities.get(i);
        }
        new ArrayWritable(WordIndexEntryWritable.class, arr).write(out);
        
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ArrayWritable aw = new ArrayWritable(WordIndexEntryWritable.class);
        aw.readFields(in);
        for (Writable w : aw.get()) {
            add((WordIndexEntryWritable) w);
        }
        
    }

    @Override
    public int compareTo(WordIndexWritable o) {
        if (this.entities.size() != o.entities.size()) return this.entities.size() - o.entities.size();
        else {
            for (int i = 0; i<entities.size(); i++) {
                if (entities.get(i).compareTo(o.entities.get(i)) != 0) {
                    return entities.get(i).compareTo(o.entities.get(i));
                }
            }
            return 0;
        }
    }

    @Override
    public String toString() {
        String res = "";
        for (WordIndexEntryWritable e : entities) {
            res += e.toString() + " ";
        }
        return res;

    }
    
}
