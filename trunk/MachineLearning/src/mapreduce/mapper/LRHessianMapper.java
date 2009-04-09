package mapreduce.mapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import ml.algorithms.utils.LRUtil;
/*
 * The Map reduce class responsible for parallelising Logistic Regression algorithms
 * 
 * */
public class LRHessianMapper {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> { 
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
		    				System.out.println("cachePath::"+cachePath.getName());//ATTN this is the local cache path... Not on HDFS...
		    				break;
		    			}
		    		}
		    	}else
		        	System.out.println("BAD  BAD CACHE!");
		    }catch (IOException ioe) {
		      System.err.println("IOException reading from distributed cache");
		      System.err.println(ioe.toString());
		    }
		  }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			 //by default the key is the byte offset of the value in the file.??
			 String phi_n_t = value.toString();//we know only one value will come in for now... Need to chage this later
			 //we will need to make the values as an Iterator<Writable> 
			 String[] featureTargetArray= phi_n_t.split(" ");//break into tokens
			 double[] phi_n = new double[featureTargetArray.length-1];//one less because target is the last value.
			 double[] w = new double[phi_n.length];//get the initial weights
			 double target = Double.parseDouble(featureTargetArray[featureTargetArray.length-1]);//target at the end...
			 BufferedReader wordReader = new BufferedReader(new FileReader(cachedModelPath.toString()));
			 int count=0;
			 String line = wordReader.readLine();
			
			 while(line!=null){
				 System.out.println("We red in the line: "+line);
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
			 System.out.println("*************inited running the lr on one daya point*********");
			 System.out.println("In hessian mapper# "+key);
			 //Great we avoided the need for a huge temporary matrix completely!. We just need to collect the 
			 //H(i,j) directly in the for loop below. This saves RAM. Even for a 50*50 double hessian assuming 
			 // it takes 2500*8 = 20000 =20KB of temp ram memory/mapper. The O/P to the HDFS is unaffected though... 
			 Double constant =LRUtil.efficientHessianConstant(w, phi_n);
			 for(int i =0;i< phi_n.length;i++){
				 for(int j =0;j< phi_n.length;j++){
					 Double temp =LRUtil.efficientHessianVariable(constant,phi_n, i, j);
					 if(Double.isNaN(temp)){
						 temp = Double.MIN_VALUE;
					 } 
					//all mappers collect the H(i,j) here. We will sum it up in the reducers
					 output.collect(new Text(i+" "+j),new DoubleWritable(temp));
					 
				 }
			 }
			 System.out.println();
			 System.out.println("Exiting mapper# "+key);
			 
			 
		}
		
	}
		
}

