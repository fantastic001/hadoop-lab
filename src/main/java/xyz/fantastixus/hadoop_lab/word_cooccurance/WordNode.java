package xyz.fantastixus.hadoop_lab.word_cooccurance;

import java.util.HashMap;
import java.util.Map;

public class WordNode {
    private String word;
    private int count;
    private Map<String, Integer> neighbours; 

    public WordNode(String word) {
        this.word = word;
        this.count = 0;
        this.neighbours = new HashMap<>();
    }
    public void addLink(String word) {
        this.count ++;
        if (this.neighbours.containsKey(word)) {
            neighbours.put(word, neighbours.get(word) + 1);
        }
        else {
            neighbours.put(word, 1);
        }
    }

    public void merge(WordNode node) {
        this.count = this.count + node.count;
        for (String w : node.neighbours.keySet()) {
            if (neighbours.containsKey(w)) {
                neighbours.put(w, neighbours.get(w) + node.neighbours.get(w));
            }
            else {
                neighbours.put(w, node.neighbours.get(w));
            }
        }
    }

    public int getCount() {
        return count;
    }
    public String getWord() {
        return word;
    }
    public Map<String, Integer> getNeighbours() {
        return neighbours;
    }
    public void setWord(String word) {
        this.word = word;
    }
    public void setCount(int count) {
        this.count = count;
    }
    public void setNeighbours(Map<String, Integer> neighbours) {
        this.neighbours = neighbours;
    }
    
}
