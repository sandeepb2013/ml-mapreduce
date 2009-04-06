package mapreduce.recordreaders;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
/*
 * A special purpose class that reads a space delimited Matrix into the system.
 * Key is i,j in text format and Value is the value at the point
 * */
public class MatrixInputReader   implements RecordReader<Text, DoubleWritable>{

	  private LineRecordReader lineReader;
	  private int iterator_row =0;
	  private int iterator_column =0;
	  private int limit =0;
	  String[] currentRecordSet;
	  
	  public MatrixInputReader(JobConf job, FileSplit split) throws IOException {
		    lineReader = new LineRecordReader(job, split);
		    currentRecordSet=null;
		  }

	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public DoubleWritable createValue() {
		return new DoubleWritable();
	}

	@Override
	public long getPos() throws IOException {
		return lineReader.getPos();

	}

	@Override
	public float getProgress() throws IOException {
		 return lineReader.getProgress();

	}

	public boolean next(Text key, DoubleWritable value) throws IOException {
		
		if(currentRecordSet ==null|| limit==0 || iterator_column ==limit-1 ){//If its the first time or we have read in all the columns for this row
			LongWritable lineKey=null;
		    Text lineValue=null;
			if ( !lineReader.next(lineKey, lineValue)) {//read in a new line
				return false;
		    }
			this.currentRecordSet = lineValue.toString().split(" ");
			if(limit!=0){
				iterator_row++;//our first time thru the data so don't increment then
			}
			limit = this.currentRecordSet.length-1;//-1 because the last row is the set of labels we dont want to read in
			iterator_column=0;// we are starting from a new line
			value = new DoubleWritable(Double.parseDouble(currentRecordSet[iterator_column].trim()));
			key =new Text((iterator_row)+","+(iterator_column++));
			return true;
			
		}else{//The line has not been read completely keep reading
			value = new DoubleWritable(Double.parseDouble(currentRecordSet[iterator_column].trim()));
			key =new Text(iterator_row+","+iterator_column);
			iterator_column++;
			return true;
		}
		//Code should never get here, in reality it should terminate in the if above...
	}

}
