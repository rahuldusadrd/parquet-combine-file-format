package com.parquet.format;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class ParquetCombineFileInputFormat extends CombineFileInputFormat<Text, Text> {

	public ParquetCombineFileInputFormat() {
		super();
		setMaxSplitSize(2000);
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader<Text, Text>((CombineFileSplit) split, context,ParquetCombineFileRecordReader.class);
	}

	
}