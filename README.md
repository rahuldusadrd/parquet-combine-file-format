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



