package mapreduce.FileFormat;
import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class SLInputFileFormat implements InputFormat, JobConfigurable {

	@Override
	public RecordReader getRecordReader(InputSplit arg0, JobConf arg1, 
			Reporter arg2) throws IOException {
		
		return null;
	}

	@Override
	public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
		
		return null;
	}

	@Override
	public void configure(JobConf arg0) {
		

	}

}
