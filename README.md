
# Running

to run test app 

    ./start-container.sh
    HADOOP_CLASSPATH=/code/target/hlab-1.0-SNAPSHOT.jar hadoop xyz.fantastixus.hadoop_lab.App /code/ncdc/input/ output

Running word frequency job

    ./start-container.sh
    HADOOP_CLASSPATH=/code/target/hlab-1.0-SNAPSHOT.jar hadoop xyz.fantastixus.hadoop_lab.wordfreq.WordFreqApp words.txt output

Word co-occurance

    ./start-container.sh
    HADOOP_CLASSPATH=/code/target/hlab-1.0-SNAPSHOT.jar hadoop xyz.fantastixus.hadoop_lab.word_cooccurance.WCApp text.txt output

Word index
    
    ./start-container.sh
    HADOOP_CLASSPATH=/code/target/hlab-1.0-SNAPSHOT.jar hadoop xyz.fantastixus.hadoop_lab.word_index.WIApp text.txt output
