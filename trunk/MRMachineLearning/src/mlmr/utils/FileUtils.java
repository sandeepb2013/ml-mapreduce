package mlmr.utils;

/*
 * A simple collection of utilities to read and write to the Hadoop file system on the fly.
 * Most utilities read and write a sequence file. Which is a binary file containing key:value pair of data.
 * */

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class FileUtils {
	/**
	 * Function I/P is the URI on the HDFS and the conf  is the job conf.
	 * The function takes the directory where the O/P path is not specific to the JobConf. The conf is passed as an 
	 * optional argument for getting the filesystem object.  The file here is specific to the 
	 * */
	public static SequenceFile.Reader getSequenceReader(String directoryPath, JobConf conf){
		FileSystem fs=null;
		try {
			fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(directoryPath));
			for(int i =0; i< status.length;i++){
				System.out.println("Found file: "+status[i].getPath());
			}
			System.out.println("In FileUtils class. Opening reader to file: "+status[1].getPath());
			
			SequenceFile.Reader sr = new SequenceFile.Reader(fs,status[1].getPath(),new JobConf(false) );
			
			return sr;
			
		} catch (IOException e) {
			System.err.println("Unable to open sequence files at URI:"+ directoryPath);
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 
	 * */
	public static SequenceFile.Reader[] getSequenceReaders(String directoryPath, JobConf conf){
		FileSystem fs=null;
		try {
			fs = FileSystem.get(conf);
			FileStatus[] tempStatus = fs.listStatus(new Path(directoryPath));
			ArrayList<FileStatus> status = new ArrayList<FileStatus>();
			
			int count=0;
			for(int i =0; i< tempStatus.length;i++){
				System.out.println("Found file: "+tempStatus[i].getPath());
				if(tempStatus[i].getPath().toString().contains("/part-")){
					count++;
					status.add(tempStatus[i]);
					System.out.println("Added to the filelist "+tempStatus[i].getPath() );
				}
			}
			SequenceFile.Reader[] sr=new SequenceFile.Reader[count];
			return status.toArray(sr);
			
		} catch (IOException e) {
			System.err.println("Unable to open sequence files at URI:"+ directoryPath);
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * This method should get the specified file by path...
	 * */
	public static SequenceFile.Reader getSequenceReaderFromFile(String filePath, JobConf conf){
		FileSystem fs=null;
		try {
			fs = FileSystem.get(conf);
			System.out.println("In FileUtils class. Opening reader to file: "+filePath);
			SequenceFile.Reader sr = new SequenceFile.Reader(fs,new Path(filePath).makeQualified(fs),new JobConf(false) );
			return sr;
			
		} catch (IOException e) {
			System.err.println("Unable to open sequence files at URI:"+ filePath);
			e.printStackTrace();
		}
		return null;
	}
	/*
	 * Overloaded method that gets files written to the for an o/p of the specific task.
	 * */
	public static SequenceFile.Reader getSequenceReader(JobConf conf){
		FileSystem fs=null;
		Path path =null;
		try {
			fs = FileSystem.get(conf);
			path = SequenceFileOutputFormat.getOutputPath(conf);
			FileStatus[] status = fs.listStatus(path);
			System.out.println("In FileUtils class. Opening reader to file: "+status[1].getPath());
			SequenceFile.Reader sr = new SequenceFile.Reader(fs,status[1].getPath(),new JobConf(false) );
			
			return sr;
			
		} catch (IOException e) {
			System.err.println("Unable to open sequence files at O/P path: "+ path);
			e.printStackTrace();
		}
		return null;
	}
	/*
	 * Open a writer object to a specific file in the hadoop filesystem.
	 * */
	public static<K,V extends Writable> SequenceFile.Writer getSequenceWriter(String URI,Class<K> keyClass, Class<V> valueClass,JobConf conf){
		FileSystem fs=null;
		try {	
			fs = FileSystem.get(conf);
			Path path = new Path(URI);
			System.out.println("Opening Sequence file for writing: "+path);
			/*SequenceFile.Writer swrt = new SequenceFile.Writer(fs, conf, 
					path,IntWritable.class, DoubleWritable.class);*/ 
			SequenceFile.Writer swrt = SequenceFile.createWriter(fs, conf, path, 
					IntWritable.class, 
					DoubleWritable.class);
			return swrt;
			
		} catch (IOException e) {
			System.err.println("Unable to open sequence files at URI:"+ URI);
			e.printStackTrace();
		}
		return null;
		
	}
	public static void closeReader(SequenceFile.Reader sr){
		try{
		sr.close();
		}catch(IOException e){
			System.err.println("Unable to close sequence file.");
			e.printStackTrace();
		}
	}
	public static void deleteFileAtURI(String URI, JobConf conf){
		FileSystem fs=null;
		try {
			fs = FileSystem.get(conf);
			fs.delete(new Path(URI), true);//make sure you delete the gradient
		}catch (IOException e){
			

		}
	}
	/*
	 * My jazzy experiment with generics for this Function.
	 * The function is basically supposed to retrieve key value pairs from a "sequence" file in an indexed array.
	 * The sequence file reader is an abstract implementation returns key value pairs from a file with an iterator
	 * and also have the type information about it. This is a special function that will read only key types that 
	 * are indexed by Integers and The value is any type of real number. How type safe do we need to make this?
	 * If the key and value are of incorrect type Exceptions will be thrown any ways... So we can just handle em(without generics)...
	 * See generics are useful only when:
	 *  1)We know that the function arguments/Return type of the call can change from time to time.
	 *  2)The Class declaration and all its methods may be have methods that deal with a parameterizable type.
	 *  So what is the parameter that will change in this method? most likely the return type. the return type will change. 
	 * */
	public static<V extends Writable> V[] readIndexedSequenceFiles(JobConf conf, Class<V> valueClass, Reader reader){
		IntWritable currkey =  new IntWritable(); //Must be fixed
		try{
		V value = valueClass.newInstance();//.newInstance();
		
		FileSystem fs = FileSystem.get(conf);
		
		TreeMap<Integer, V> temp = new TreeMap<Integer, V>();
		
		while(reader.next(currkey,value)){
			temp.put(currkey.get(), value);
			value = valueClass.newInstance();
		}
		V[] array = (V[]) Array.newInstance(valueClass, temp.size());//Use reflection to get the correct array
		for (int i = 0; i < array.length; i++) {
			array[i]= temp.get(new Integer(i));
		}
		return array;
		
		}catch(IOException e){
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
		
	}

	
}