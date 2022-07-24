package xyz.fantastixus.hadoop_lab.tweet_distance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class DLDistance {
    private String base; 
    public DLDistance(String base) {
        this.base = base; 
    }

    public int get(String b) {
        String a = base; 
        int DA_SIZE = 128;
        int[] da = new int[DA_SIZE];
        int i;
        for (i=0; i<DA_SIZE; i++) {
            da[i] = 0;
        }
    
        int[][] d = new int[a.length()+2][b.length()+2];
    
        int maxdist = a.length() + b.length();
        d[0][0] = maxdist;
        for (i=1; i<=a.length()+1; i++) {
            d[i][0] = maxdist;
            d[i][1] = i-1;
        }
        // for j := 0 to length(b) inclusive do
        // d[−1, j] := maxdist
        // d[0, j] := j
        for (i=1; i<=b.length()+1; i++) {
            d[0][i] = maxdist;
            d[1][i] = i-1;
        }
    
        // for i := 1 to length(a) inclusive do
        // db := 0
        // for j := 1 to length(b) inclusive do
        //     k := da[b[j]]
        //     ℓ := db
        //     if a[i] = b[j] then
        //     cost := 0
        //     db := j
        //     else
        //     cost := 1
        //     d[i, j] := minimum(d[i−1, j−1] + cost,  //substitution
        //                d[i,   j−1] + 1,     //insertion
        //                d[i−1, j  ] + 1,     //deletion
        //                d[k−1, ℓ−1] + (i−k−1) + 1 + (j-ℓ−1)) //transposition
        // da[a[i]] := i
        // return d[length(a), length(b)]
        for (i = 1; i<=a.length(); i++) {
            int db = 0;
            for (int j = 1; j<=b.length(); j++) {
                int k = da[(int) b.charAt(j-1)-1];
                int l = db;
                int cost;
                if (a.charAt(i-1) == b.charAt(j-1)) {
                    db = j; 
                    cost = 0;
                }
                else {
                    cost = 1;
                }
                d[i+1][j+1] = minimum(
                    d[i][j] + cost,
                    d[i+1][j] + 1,
                    d[i][j+1] + 1,
                    d[k][l] + (i - k - 1) + 1 + (j-l-1)
                );
            }
            da[(int) a.charAt(i-1)-1] = i;
        }
        return d[a.length()+1][b.length()+1];
    
    }

    public int minimum(int a, int b, int c, int d) {
        return Collections.min(Arrays.asList(a,b,c,d));
    }
}
