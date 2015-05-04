# hadoop-playground
Playground for Hadoop-related stuff

## Counting entries in a single WARC file

    $hadoop jar de.tudarmstadt.ukp.dkpro.hadoop.warc-0.1-SNAPSHOT.jar \
    de.tudarmstadt.ukp.dkpro.hadoop.warc.WarcRecordCounter \
    /path/to/file.warc.gz /outdir
    
Showing the result:

    $hadoop fs -cat /outdir/*

    WARC records	11278
   