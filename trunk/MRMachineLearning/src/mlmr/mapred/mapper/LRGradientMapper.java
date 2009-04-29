package mlmr.mapred.mapper;


import java.io.IOException;

import java.util.Arrays;

import java.util.Random;



import mlmr.mapred.writable.DoubleArrayWritable;
import mlmr.utils.FileUtils;
import mlmr.utils.LRUtil;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.mapred.Reporter;


/*
 * The Map reduce class responsible for parallelizing Logistic Regression algorithms
 * 
 * */
public class LRGradientMapper {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> { 
	   
	  
	   private JobConf conf;
	   private static DoubleWritable[] weights=null;//universal model
	   private boolean controlFlag1=true;
	   
	   public void configure(JobConf conf) {
		    
				this.conf=conf;
				String controlFlag = conf.get("controlFlag1",null );//get the path
				System.out.println("In Gradient mapper. The control flag from driver is:: "+conf.get("controlFlag1",null ));
				System.out.println("In Gradient mapper. The control flag for Mapper is:: "+controlFlag1);
				if(controlFlag.equals("false")){
					System.out.println("In gradient mapper Getting the weights now");
					String weightsFile = conf.get("weightsFile",null );//get the path
					//IntWritable currkey =  new IntWritable(); 
				 	SequenceFile.Reader sr = FileUtils.getSequenceReaderFromFile(weightsFile, conf);
				 	//DoubleWritable currValue = new DoubleWritable();
				 	
				 	DoubleWritable[] weightsArray = FileUtils.readIndexedSequenceFiles(conf,DoubleWritable.class,sr);
				 	
				 	weights =(DoubleWritable[]) Arrays.asList(weightsArray).toArray();
				 	try {
						sr.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}else if(controlFlag.equals("true")){//this is the first iteration all weights must be zero
					 System.out.println("In gradient mapper. looks like this is the first iteration");
					 weights = null;
					
				}
	   }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter) throws IOException {
			 //by default the key is the byte offset of the value in the file.??
			 String phi_n_t = value.toString();//we know only one value will come in for now... Need to chage this later
			 //we will need to make the values as an Iterator<Writable> 
			 String[] featureTargetArray= phi_n_t.split(" ");//break into tokens
			 DoubleWritable[] phi_n = new DoubleWritable[featureTargetArray.length-1];//one less because the target is the last value
			 DoubleWritable target = new DoubleWritable(Double.parseDouble(featureTargetArray[featureTargetArray.length-1]));//target at the end...
			
			 
			if(weights==null){
					weights = new DoubleWritable[phi_n.length];
			}
			System.out.print("Weightsof length : "+weights.length);
			for(int i=0 ;i < phi_n.length ;i++){
				phi_n[i] = new DoubleWritable(Double.parseDouble(featureTargetArray[i]));
				System.out.print("B "+weights[i]+", ");
				if(weights[i]==null||weights[i].get()==0){
					weights[i]=new DoubleWritable();
					weights[i].set(0);
				}
				System.out.print("A "+weights[i].get()+", ");
			}System.out.println();
			 
			 //////////////////////////////GRADIENT CALCULATED HERE///////////////////////////////
			 DoubleWritable[] gradient= LRUtil.calculateGradient(weights, target, phi_n);//later we will call this on an adaptor interface...
			 /////////////////////////////////////////////////////////////
			 for(int i =0;i< gradient.length;i++){
			   System.out.print("GradVAL "+i+"::"+gradient[i]+"   ");
			 }System.out.println();
			 int i = new Random().nextInt(6);//generate a number between 0 to n-1 inclusive
			 
			 //now we need to write the O/p keys
			 DoubleArrayWritable ars = new DoubleArrayWritable();
			 ars.set(gradient);
			 try{
			 output.collect(new IntWritable(i),ars);//collect the values into one of the 1 bin... Only one reducer to be spawned...
			 System.out.println("Exiting Gradient mapper# "+key);
			 }catch(Exception e){
				 System.out.println("In gradient mapper Exception while writing to the collector."+" value :"+ars);
				 e.printStackTrace();
			 }
			 
		}
		
	}
}
