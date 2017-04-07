###################### PAGE RANK ############################

******* For wiki Main Execution ***********
For defining the input location (steps 1 to 5)
1. sudo su hdfs
2. hadoop fs -mkdir /user/cloudera
3. hadoop fs -chown cloudera /user/cloudera
4. Exit
5. hadoop fs -mkdir /user/cloudera/PageRank /user/cloudera/PageRank/input
6. Uploading the wiki main file in the input location
hadoop fs -put /home/cloudera/PageRank/simplewiki-20150901-pages-articles-processed.xml   /user/cloudera/PageRank/input
7. Assuming that jar file is created through eclipse, therefore for executing the code use following instruction
hadoop jar Driver.jar pagerank.Driver /user/cloudera/PageRank/input /user/cloudera/PageRank/output
8. Taking page ranking of wiki corpus to text file
hadoop fs -get /user/cloudera/PageRank/output/filtersort/part-r-00000 wikimainfinal.txt

******* For wiki micro Execution ***********
1.Using the same location, so remove the main wiki file from input
hadoop fs -rm -r /user/cloudera/PageRank/input/simplewiki-20150901-pages-articles-processed.xml
2.Load the micro-wiki.txt file in input
adoop fs -put /home/cloudera/PageRank/wiki-micro.txt   /user/cloudera/PageRank/input
3. Delete the previous output path
hadoop fs -rm -r /user/cloudera/PageRank/output/*
4. Execution
hadoop jar Driver.jar pagerank.Driver /user/cloudera/PageRank/input /user/cloudera/PageRank/output
5. Taking page ranking of wiki corpus micro to text file
PageRank]$ hadoop fs -get /user/cloudera/PageRank/output/filtersort/part-r-00000 wikismallfinal.txt

################################################################