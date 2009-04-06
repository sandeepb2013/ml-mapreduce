package mapreduce.InputFormat;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/**
 * Gradient calculation will require line by line 
 * */
public class GradientInputFormat extends FileInputFormat<LongWritable,Text>{

	@Override
	  public RecordReader<LongWritable, Text> getRecordReader(
              InputSplit genericSplit, JobConf job,
              Reporter reporter)throws IOException {

		reporter.setStatus(genericSplit.toString());
		return new LineRecordReader(job, (FileSplit) genericSplit);
	  }

}
