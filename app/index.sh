#!/bin/bash
echo "Mapreduce jobs using hadoop streaming to index documents"

# remove previous result
hadoop fs -rm -r /user/root/tmp/document_frequency_word_within_one_doc.output1

# no spaces near comma
# automatic sort -k 1
# no comments between lines
# errors like "Unexpected EOF" are ok
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files ./mapreduce/mapper1.py,./mapreduce/reducer1.py \
  -archives ./.venv.tar.gz#.venv \
  -D mapreduce.reduce.memory.mb=2048 \
  -D mapreduce.reduce.java.opts=-Xmx1800m \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py" \
  -input /data/* \
  -output /user/root/tmp/document_frequency_word_within_one_doc.output1 \
  -numReduceTasks 1 2>&1 | tee mr_job1.log

# instead use requests to download
# hdfs dfs -put './stopwords.txt' /data

# Dont work
# mapred streaming \
# -files ./mapreduce/mapper1.py,./mapreduce/reducer1.py \
# -mapper 'python3 ./mapreduce/mapper1.py' \
# -reducer 'python3 ./mapreduce/reducer1.py' \
# -input /data/* \
# -output ./tmp/document_frequency_word_within_one_doc.output1

# test mapreduce
# cat ./data/67227_A_Brief_History_of_Time.txt | python3 ./mapreduce/mapper1.py | sort -k 1 | python3 ./mapreduce/reducer1.py

# hdfs dfs -cat /user/root/tmp/document_frequency_word_within_one_doc.output1/part-00000
# hadoop fs -ls /user/root/tmp/document_frequency_word_within_one_doc.output1