package mapreduce.mapper;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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

import mapreduce.exceptions.*;
import mapreduce.mlwritables.DoubleArrayWritable;
import mapreduce.utils.FileUtils;
import ml.algorithms.utils.LRUtil;
/*
 * The Map reduce class responsible for parallelizing Logistic Regression algorithms
 * 
 * */
public class LRGradientsMapper {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> { 
	   private Path[] model;
	   private static String HDFS_LR_MODEL="/FinalLRModel/model.txt";
	   private Path cachedModelPath;
	   private JobConf conf;
	   private static ArrayList<Double> w = new ArrayList<Double>();//universal model
	   public void configure(JobConf conf) {
		    try{
				Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
				BufferedReader wordReader=null;
				if (null != cacheFiles && cacheFiles.length > 0) {
					System.out.println("Fetching paths...");
			        for (Path cachePath : cacheFiles) {
			        	System.out.println("PATHS: "+cachePath);
			          if (cachePath.getName().equals(HDFS_LR_MODEL)) {
			        	wordReader = FileUtils.getResouceAsReader(cachePath, conf);
			            break;
			          }
			        }
				}
			 int count=0;
			 System.out.println("Object WR="+wordReader);
			 if(wordReader!=null){
				 String line = wordReader.readLine();
				 System.out.println("We red in the line: "+line);
				 
				 while(line!=null){
					 String[] split = line.split(" ");
					 w.add(Double.parseDouble(split[1].trim()));//Read in the model;
					 count++;
					 line = wordReader.readLine();
				 }
			 }else{//Reader is null....
				 System.out.println("wordReader"+wordReader);
				 System.out.println("Could not read in the model using zero matrix!!");
				 
			 }
			}catch(IOException e){
				System.out.println("Could not read in the model using zero matrix!!");
				e.printStackTrace();
			}
			 if(w.size()<=0){//the model was not read in this is initialisation 
				 System.out.println("Model was empty. This could be initialization");
			}
			
		    
		  }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter) throws IOException {
			 //by default the key is the byte offset of the value in the file.??
			 String phi_n_t = value.toString();//we know only one value will come in for now... Need to chage this later
			 //we will need to make the values as an Iterator<Writable> 
			 String[] featureTargetArray= phi_n_t.split(" ");//break into tokens
			 double[] phi_n = new double[featureTargetArray.length-1];//one less because the target is the last value
			 double target = Double.parseDouble(featureTargetArray[featureTargetArray.length-1]);//target at the end...
			 for(int i=0 ;i < phi_n.length ;i++){
				 phi_n[i] = Double.parseDouble(featureTargetArray[i]);
				 System.out.print("VAL"+i+"::"+phi_n[i]+"   ");
			 }
			 System.out.println("*************Data inited. Running the lr on one data point*********");
			 System.out.println("In Gradient mapper# "+key);
			 double[] weights = new double[phi_n.length];
			 int count=0;
			 if(w.size()==phi_n.length){
				 for (Double d : w) 
					 weights[count++] =d;
			 }else{
				 Arrays.fill(weights, new Double(0));
			 }
			 
			 double[] gradient= LRUtil.calculateGradient(weights, target, phi_n);//later we will call this on an adaptor interface...
			 
			 DoubleWritable[] update = new DoubleWritable[gradient.length];//don't convert this back to string!
			 for(int i =0;i< gradient.length;i++){
				if(Double.isNaN(gradient[i])){
					gradient[i] = Double.MIN_VALUE;
				 } 
				update[i] = new DoubleWritable(gradient[i]);
			    System.out.print("VAL "+i+"::"+update[i]+"   ");
			 }System.out.println();
			 System.out.println("*************Writing the above O/p to reducers *********");
			 int i = new Random().nextInt(6);//generate a number between 0-19 inclusive
			 
			 //now we need to write the O/p keys
			 DoubleArrayWritable ars = new DoubleArrayWritable();
			 ars.set(update);
			 output.collect(new IntWritable(1),ars);//collect the values into one of the 1 bin... Only one reducer to be spawned...
			 System.out.println("Exiting mapper# "+key);
			 
			 
		}
		
	}
}

