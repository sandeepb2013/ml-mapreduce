package mapreduce.InputFormat;

import java.io.IOException;

import mapreduce.recordreaders.MatrixInputReader;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
/*
 * 
 * */
public class MatrixIndexInputFormat extends FileInputFormat<Text,DoubleWritable> {

	@Override
	  public RecordReader<Text, DoubleWritable> getRecordReader(
              InputSplit genericSplit, JobConf job,
              Reporter reporter)throws IOException {

		reporter.setStatus(genericSplit.toString());
		return new MatrixInputReader(job, (FileSplit) genericSplit);
	  }

}
