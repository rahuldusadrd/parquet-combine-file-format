package com.parquet.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.example.data.Group;

import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class ParquetCombineFileRecordReader extends RecordReader<Text, Text> {

	private final FileSplit filesplit;
	
	private final Text key = new Text();

	private final ParquetRecordReader<Group> parquetRecordReader;
	
	

	public ParquetCombineFileRecordReader(
			FileInputFormat<Text, Text> inputFormat, CombineFileSplit split,
			Configuration conf, Reporter reporter, Integer index)
			throws IOException {
		
		filesplit = new FileSplit(split.getPath(index), split.getOffset(index),split.getLength(index), split.getLocations());		
		ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, split.getPath(0), ParquetMetadataConverter.NO_FILTER);
	    MessageType schema = readFooter.getFileMetaData().getSchema();
	    System.out.println(schema.getColumns());
	    GroupReadSupport readSupport = new GroupReadSupport();
		parquetRecordReader = new ParquetRecordReader<Group>(readSupport);
	}

	public ParquetCombineFileRecordReader(CombineFileSplit split,TaskAttemptContext context, Integer index) throws IOException

	{
		
		filesplit = new FileSplit(split.getPath(index), split.getOffset(index),split.getLength(index), split.getLocations());		
	    GroupReadSupport readSupport = new GroupReadSupport();
		parquetRecordReader = new ParquetRecordReader<Group>(readSupport);
	}

	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		parquetRecordReader.initialize(filesplit, context);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return parquetRecordReader.nextKeyValue();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		StringBuilder value = new StringBuilder();
		Group g = parquetRecordReader.getCurrentValue();
		int fieldCount = g.getType().getFieldCount();
		for (int field = 0; field < fieldCount; field++) {
		      int valueCount = g.getFieldRepetitionCount(field);
		 
		      Type fieldType = g.getType().getType(field);
		      String fieldName = fieldType.getName();
		 
		      for (int index = 0; index<valueCount; index++) {
		        if (fieldType.isPrimitive()) {
		        	value.append(fieldName+"~"+g.getValueToString(field, index)+"\t");
		        }
		      }
		    }
		return new Text(value.toString());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return parquetRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		parquetRecordReader.close();
	}
	
	
	
}
