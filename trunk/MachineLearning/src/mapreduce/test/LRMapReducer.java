package mapreduce.test;
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

import mapred.mlwritables.DoubleArrayWritable;
import mapreduce.exceptions.*;
/*
 * The Map reduce class responsible for parallelizing Logistic Regression algorithms
 * 
 * */
public class LRMapReducer {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> { 
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
		    } catch (IOException ioe) {
		      System.err.println("IOException reading from distributed cache");
		      System.err.println(ioe.toString());
		    }
		  }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter) throws IOException {
			 //by default the key is the byte offset of the value in the file.??
			 String phi_n_t = value.toString();//we know only one value will come in for now... Need to chage this later
			 //we will need to make the values as an Iterator<Writable> 
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
			 System.out.println("*************inited running the lr on one daya point*********");
			 System.out.println("In mapper# "+key);
			 
			 double[] gradient= LRUtils.calculateGradient(w, target, phi_n);//later we will call this on an adaptor interface...
			 double[][] hessian = LRUtils.hessian(w, phi_n);//this too...
			 
			 DoubleWritable[] update = new DoubleWritable[gradient.length];//don't convert this back to string!
			 for(int i =0;i< gradient.length;i++){
				double temp = (1/hessian)*gradient[i];
				if(Double.isNaN(temp)){
					 temp = Double.MIN_VALUE;
				 } 
				update[i] = new DoubleWritable(temp);
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
		public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleArrayWritable, IntWritable, Text> {
			 	    
			@Override
			public void reduce(IntWritable key, Iterator<DoubleArrayWritable> values,
					OutputCollector<IntWritable, Text> output, Reporter reporter)
					throws IOException  {
					System.out.println("In reducer# "+key);
					DoubleWritable[] firstStageSum =null;//sum of all firstStageElements arrays
					Writable[] firstStageElements=null;
					if(values.hasNext()){//get each array into firstStageElements
						firstStageElements = values.next().get();
						firstStageSum = new DoubleWritable[firstStageElements.length];
					}
					for(int i = 0 ; i< firstStageElements.length ;i++){
						firstStageSum[i]=new DoubleWritable(0.0);
					}
					while(values.hasNext()){//add each of the values of firstStageElements to firststagesum
						firstStageElements = values.next().get();//get the new array from the map process
						for(int j=0;j<firstStageElements.length;j++){
							System.out.print("VALR: "+j+" :"+firstStageElements[j]+"  ");
						}System.out.println();
						System.out.println(" firstStageElements.length: "+firstStageElements.length+" firstStageSum.size: "+firstStageSum.length);
						//add it to the sum
						for(int i = 0 ;i <firstStageElements.length;i++){
							firstStageSum[i]= new DoubleWritable(firstStageSum[i].get() +(double)((DoubleWritable)firstStageElements[i]).get());
							
						}
						for(int j=0;j<firstStageSum.length;j++){
							System.out.print("VALF: "+j+" :"+firstStageSum[j]+"  ");
						}System.out.println();
						
					}
					List ls = Arrays.asList(firstStageSum);
					Text s = new Text(ls.toString());
					System.out.println("Reduce O/p:"+ls.toString());
					output.collect(key, s);//Write o/p to the file.
					System.out.println("exiting reducer# "+key);
				
			}
		}

	}

