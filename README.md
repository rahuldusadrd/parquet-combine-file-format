# Parquet Combine File Format
This project contains combine file format for Parquet Input Format. This format is useful in a use case when there are lot of small sized parquet files to process.


Problem with Map Reduce/Spark processing of small files
---------------------------------------------------------------------------------------------------------------------------------------
Processing small files is an old typical problem when processed under most of the Big Data technologies like Map Reduce, Spark etc. Here small file means a file which is significantly smaller than the HDFS block size (default 64MB/128MB). Due to small files too many tasks are spawned and that increases over all job time.

Increasing the minimum split size, to reduce the number of map tasks, will not solve the issue as Splits are calculated per file basis.The problem of small files processing becomes even more difficult when underlying input format is parquet.

Solution
----------------------------------------------------------------------------------------------------------------------------------------

Solution to such scenario is to build CombineFileFormat for parquet files which can club small sized files under one format.

Input data snapshot
----------------------------------------------------------------------------------------------------------------------------------------
![alt text](https://github.com/rahuldusadrd/parquet-combine-file-format/blob/master/images/data.png)



Download parquet files from below location and put them on hdfs directory which is "/input/parquet-files" in our case.

Parquet test files location:- https://github.com/rahuldusadrd/parquet-combine-file-format/blob/master/test-data/employee/

Commands To put them in HDFS:-
----------------------------------------------------------------------------------------------------------------------------------------
hadoop fs -mkdir /input/parquet-files

hadoop fs -copyFromLocal <local-dir>/employee/* /input/parquet-files

Create the jar of parquet-combine-file-format and start the spark shell with below command
-----------------------------------------------------------------------------------------------------------------------------------------
spark-shell --master yarn --jars <jar location>/parquet-combine-file-format-1.0-SNAPSHOT-jar-with-dependencies.jar
  
Run the following commands on spark shell to validate the data is loaded correctly or not

sqlContext.read.parquet("hdfs://localhost:54310/input/parquet-files").show


To check the number of partitions

sqlContext.read.parquet("hdfs://localhost:54310/input/parquet-files").rdd.getNumPartitions

Below out will be produced:-

3


Now use the customize input format to load the data and see the difference:-
----------------------------------------------------------------------------------------------------------------------------------------

//import statements

import org.apache.hadoop.io.Text
import com.parquet.format.ParquetCombineFileInputFormat

// Creating RDD using custom input format

val input = sc.newAPIHadoopFile("hdfs://localhost:54310/input/parquet-files/", classOf[ParquetCombineFileInputFormat], classOf[Text],classOf[Text])

val df = input.map(x=>x._2.toString).map{emp=>
val values = emp.split("\t")
(values(0),values(1),values(2))
}.toDF("id","name","salary")


df.rdd.getNumPartitions


The output will be

1



So number for partitions are reduced from 3 to 1. This will improve the performace of overall job.

