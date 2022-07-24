package xyz.fantastixus.hadoop_lab.word_index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.nimbusds.jose.util.ArrayUtils;

public class WordIndexEntry implements Comparable<WordIndexEntry>{
    private String word; 
    private String filename;
    private ArrayList<Integer> positions;

    public WordIndexEntry(String word, String filename) {
        this.word = word;
        this.filename = filename;
        this.positions = new ArrayList<>();
    }

    public void addPosition(int pos) {
        this.positions.add(pos);
    }

    @Override
    public String toString() {
        String result = "";
        for (int pos : positions) {
            result += filename + ":" + Integer.valueOf(pos) + " ";
        }
        return result;
    }

    public String getWord() {
        return word;
    }
    public String getFilename() {
        return filename;
    }
    public ArrayList<Integer> getPositions() {
        return positions;
    }

    @Override
    public int compareTo(WordIndexEntry o) {
        int x = this.word.compareTo(o.word);
        int y = this.filename.compareTo(o.filename);
        if (x != 0) return x;
        else if (y != 0) return y; 
        else {
            if (this.positions.size() < o.positions.size()) return -1;
            else if (this.positions.size() > o.positions.size()) return 1;
            else {
                Object[] a = positions.toArray();
                Object[] b = o.positions.toArray();
                for (int i = 0; i<a.length; i++) {
                    int a1 = (int) a[i];
                    int b1 = (int) b[i];
                    if (a1 != b1) return a1 - b1;
                }
                return 0;

            }
        }
    }
    
}
