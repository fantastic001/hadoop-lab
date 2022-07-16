#!/bin/bash 

DATA=$1

for year in $DATA/all/*; do 
    echo -ne "$(basename $year .gz) "
    gunzip -c $year | \
        awk "
            {
                temp = substr(\$0, 88, 5) + 0;
                q = substr(\$0, 93, 1);
                if (temp != 9999 && q ~ /[01459]/ && temp > max) max = temp;
            }
            END {print max; }
        "
done 