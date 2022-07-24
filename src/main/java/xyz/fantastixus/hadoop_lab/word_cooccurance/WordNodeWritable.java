package xyz.fantastixus.hadoop_lab.word_cooccurance;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordNodeWritable implements WritableComparable<WordNodeWritable> {

    private WordNode node; 

    public WordNodeWritable() {
        node = null;
    }

    public WordNodeWritable(WordNode node) {
        this.node = node;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(node.getWord()).write(out);
        new IntWritable(node.getCount()).write(out);
        MapWritable mymap = new MapWritable();
        for (String w : node.getNeighbours().keySet()) {
            mymap.put(new Text(w), new IntWritable(node.getNeighbours().get(w)));
        }
        mymap.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        IntWritable myint = new IntWritable(); 
        Text mytext = new Text();
        MapWritable mymap = new MapWritable();
        mytext.readFields(in);
        myint.readFields(in);
        mymap.readFields(in);
        node = new WordNode(mytext.toString());
        HashMap<String, Integer> m = new HashMap<>();
        for (Writable w : mymap.keySet()) {
            IntWritable xx = (IntWritable) mymap.get(w);
            m.put(w.toString(), xx.get());
        }
        node.setCount(myint.get());;
        node.setNeighbours(m);

    }

    @Override
    public int compareTo(WordNodeWritable o) {
        return this.get().getWord().compareTo(o.get().getWord());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false; 
        if (! (obj instanceof WordNodeWritable)) return false; 
        WordNodeWritable o = (WordNodeWritable) obj;
        return o.get().getWord().equals(this.get().getWord()) && 
            this.get().getCount() == o.get().getCount() && 
            this.get().getNeighbours().equals(o.get().getNeighbours());
    }
    
    public WordNode get() {
        return node;
    }
}
