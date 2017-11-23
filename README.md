# parquet-format
This project contains combine file format for # Parquet Input Format. This format is useful in a use case where there are lot of small sized parquet files to process.


Problem with Map Reduce/Spark processing of small files
---------------------------------------------------------------------------------------------------------------------------------------
Processing small files is an old typical problem when processed under most of the Big Data technologies like Map Reduce, Spark etc. Here small file one which is significantly smaller than the HDFS block size (default 64MB/128MB). 




