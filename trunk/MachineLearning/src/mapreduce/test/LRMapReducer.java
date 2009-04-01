package mapreduce.test;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 * The Map reduce class responsible for parallelizing Logistic Regression algorithms
 * 
 * */
public class LRMapReducer {

	private static LRUtils lrUtils;
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> { 
	   private Path[] model;
	   private static String HDFS_LR_MODEL="/FinalLRModel/model.txt";
	   private Path cachedModelPath;
	   public void configure(JobConf conf) {
		    try {
		    	
		      String hdfsModelPath = new Path(HDFS_LR_MODEL).getName();
		      Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		      if (null != cacheFiles && cacheFiles.length > 0 ) {
		        for (Path cachePath : cacheFiles) {
		        	if( cachePath.getName().equals(hdfsModelPath)){
		        		cachedModelPath = cachePath;
		        		System.out.println("cachePath::"+cachePath.getName());//ATTN this is the local cacahe path... Not on HDFS...
		        		break;
		          }
		        }
		        }else
		        	System.out.println("BAD  BAD CACHE!");
		    } catch (IOException  ioe) {
		      System.err.println("IOException reading from distributed cache");
		      System.err.println(ioe.toString());
		    }
		  }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			 //by default the key is the byte offset of the value in the file.??
			 String phi_n_t = value.toString();
			 String[] featureTargetArray= phi_n_t.split(" ");//break into tokens
			 double[] phi_n = new double[featureTargetArray.length-1];//one less because the target is the last value
			 double[] w = new double[phi_n.length];//get the initial weights
			 double target = Double.parseDouble(featureTargetArray[featureTargetArray.length-1]);//target at the end...
			 BufferedReader wordReader = new BufferedReader(new FileReader(cachedModelPath.toString()));
			 int count=0;
			 String line = wordReader.readLine();
			 System.out.println("We red in the line: "+line);
			 while(line!=null){
				 w[count]= Double.parseDouble(line.trim());//Read in the model;
				 count++;
				 line = wordReader.readLine();
			 }
			 if(count!=phi_n.length-1 || count==0){//the model was not read in this is initialisation 
				 System.out.println("First iteration detected for mapper.");
			 }
			 for(int i=0 ;i < phi_n.length ;i++){
				 phi_n[i] = Double.parseDouble(featureTargetArray[i]);
				 System.out.print("VAL"+i+"::"+phi_n[i]+"   ");
			 }
			 System.out.println("*************inited running the lr on one set");
			 
			 double[] gradient= lrUtils.calculateGradient(w, target, phi_n);//later we will call this on an adaptor interface...
			 double hessian = lrUtils.hessian(w, phi_n);//this too...
			 //now we need to get the 
			 
			 
		}
		
	}
		public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, Text, IntWritable> {
			 	    
			@Override
			public void reduce(IntWritable key, Iterator<DoubleWritable> values,
					OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
				// TODO Auto-generated method stub
				
			}
		}

	}

