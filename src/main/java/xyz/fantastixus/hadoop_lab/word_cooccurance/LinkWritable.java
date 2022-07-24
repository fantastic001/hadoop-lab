package xyz.fantastixus.hadoop_lab.word_cooccurance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LinkWritable implements WritableComparable <LinkWritable> {

    private String from; 
    private String to; 
    private double weight;
    public LinkWritable() {
        
    }
    public LinkWritable(String from, String to, double weight) {
        this.to = to; 
        this.from = from; 
        this.weight = weight;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(from).write(out);
        new Text(to).write(out);
        new DoubleWritable(weight).write(out);
        
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Text x1 = new Text();
        Text x2 = new Text();
        DoubleWritable d1 = new DoubleWritable();
        x1.readFields(in);
        x2.readFields(in);
        d1.readFields(in);
        from = x1.toString();
        to = x2.toString();
        weight = d1.get();
    }

    @Override
    public int compareTo(LinkWritable o) {
        int x = this.from.compareTo(o.from);
        int y = this.to.compareTo(o.to);
        if (x == 0 && y == 0) {
            return (this.weight < o.weight ? -1 : 1);
        } 
        else if (x == 0) return y; 
        else return x;
    }

    @Override
    public String toString() {
        return this.to + " " + Double.valueOf(this.weight).toString();
    }
    
}
