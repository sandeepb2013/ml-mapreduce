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
import java.util.TreeMap;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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
	   
	  
	   private JobConf conf;
	   private static double[] weights=null;//universal model
	   private boolean controlFlag1=true;
	   
	   public void configure(JobConf conf) {
		    
				this.conf=conf;
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
			 }System.out.println();
			 String controlFlag = conf.get("controlFlag1",null );//get the path
			//Read in the weights file and subtract the updates from that...
			System.out.println("In Gradient mapper. The control flag from driver is:: "+conf.get("controlFlag1",null ));
			System.out.println("In Gradient mapper. The control flag for Mapper is:: "+controlFlag1);
			
			if(controlFlag.equals("false") && controlFlag1){
				System.out.println("In gradient mapper Getting the weights now");
				weights = new double[phi_n.length];
				
				String weightsFile = conf.get("weightsFile",null );//get the path
				//IntWritable currkey =  new IntWritable(); 
			 	SequenceFile.Reader sr = FileUtils.getSequenceReaderFromFile(weightsFile, conf);
			 	DoubleWritable[] weightsArray = FileUtils.readIndexedSequenceFiles(conf,DoubleWritable.class,sr);
			 	int i=0;
			 	for (DoubleWritable doubleWritable : weightsArray) {
			 		weights[i]= doubleWritable.get();
			 		i++;
				}
			 	sr.close();
			 	/*DoubleWritable currValue = new DoubleWritable();
			 	while(sr.next(currkey)){
					sr.getCurrentValue(currValue);
					weights[currkey.get()] = currValue.get();
			 		System.out.println("Weights key:: "+currkey.get()+". Weights Val:: "+currValue.get());
			 	}*/
			 	
			 	controlFlag1 = false;//reset the flag so that the common weights are not read in again and again
			 }else if(controlFlag.equals("true") && controlFlag1){//this is the first iteration all weights must be zero
				 System.out.println("In gradient mapper. looks like this is the first iteration");
				 weights = new double[phi_n.length];
				 Arrays.fill(weights, 0);
				 controlFlag1 = false;//we have inited the weights once dont want t
			 }
			
			 System.out.println("In Gradient mapper. Weights array length =  "+weights.length);
			 System.out.println("In Gradient mapper. Length ofI/P vector =  "+phi_n.length+" Phi Read in is "+phi_n_t);
			 //////////////////////////////GRADIENT CALCULATED HERE///////////////////////////////
			 double[] gradient= LRUtil.calculateGradient(weights, target, phi_n);//later we will call this on an adaptor interface...
			 /////////////////////////////////////////////////////////////
			 DoubleWritable[] update = new DoubleWritable[gradient.length];//don't convert this back to string!
			 for(int i =0;i< gradient.length;i++){
				if(Double.isNaN(gradient[i])){
					gradient[i] = Double.MIN_VALUE;
				 } 
				update[i] = new DoubleWritable(gradient[i]);
			    System.out.print("VAL "+i+"::"+update[i]+"   ");
			 }System.out.println();
			 System.out.println("*************In gradient mapper Writing the above O/p to reducers *********");
			 int i = new Random().nextInt(6);//generate a number between 0-19 inclusive
			 
			 //now we need to write the O/p keys
			 DoubleArrayWritable ars = new DoubleArrayWritable();
			 ars.set(update);
			 try{
			 output.collect(new IntWritable(1),ars);//collect the values into one of the 1 bin... Only one reducer to be spawned...
			 System.out.println("Exiting Gradient mapper# "+key);
			 }catch(Exception e){
				 System.out.println("In gradient mapper Exception while writing to the collector."+" value :"+ars);
				 e.printStackTrace();
			 }
			 
		}
		
	}
}

