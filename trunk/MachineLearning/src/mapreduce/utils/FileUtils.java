package mapreduce.utils;


import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class FileUtils {
	/**
	 * Function I/P is the URI on the HDFS and the conf  is the job conf
	 * */
	public static SequenceFile.Reader getSequenceReader(String URI, JobConf conf){
		FileSystem fs=null;
		try {
			fs = FileSystem.get(conf);
			SequenceFile.Reader sr = new SequenceFile.Reader(fs,new Path(URI),new JobConf(false) );
			return sr;
			
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
}
