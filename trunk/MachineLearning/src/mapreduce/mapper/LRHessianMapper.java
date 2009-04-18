package mapreduce.mapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

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

import mapreduce.utils.FileUtils;
import ml.algorithms.utils.LRUtil;
/*
 * The Map reduce class responsible for parallelising Logistic Regression algorithms
 * 
 * */
public class LRHessianMapper {

	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> { 
	  
		   private JobConf conf;
		   private boolean controlFlag1;
		   public void configure(JobConf conf) {
			   this.conf=conf;
			   controlFlag1=true;
			   System.out.println("Cofiguring the hessian mapper..");
		  }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			 //by default the key is the byte offset of the value in the file.??
			 String phi_n_t = value.toString();//we know only one value will come in for now... Need to chage this later
			 //we will need to make the values as an Iterator<Writable> 
			 String[] featureTargetArray= phi_n_t.split(" ");//break into tokens
			 double[] phi_n = new double[featureTargetArray.length-1];//one less because target is the last value.
			 double[] weights = new double[phi_n.length];//get the initial weights
			 double target = Double.parseDouble(featureTargetArray[featureTargetArray.length-1]);//target at the end...
			 
			 String controlFlag = conf.get("controlFlag1",null );//get the path
			 System.out.println("In Hessian mapper. The control flag is"+conf.get("controlFlag1",null ));
			 System.out.println("In Hessian mapper. The control flag form mapper is"+controlFlag1);

			if(controlFlag.equals("false") && controlFlag1){
				System.out.println("In gradient mapper Getting the weights now");
				weights = new double[phi_n.length];
				String weightsFile = conf.get("weightsFile",null );//get the path
				//IntWritable currkey =  new IntWritable(); 
			 	SequenceFile.Reader sr = FileUtils.getSequenceReaderFromFile(weightsFile, conf);
			 	//DoubleWritable currValue = new DoubleWritable();
			 	
			 	DoubleWritable[] weightsArray = FileUtils.readIndexedSequenceFiles(conf,DoubleWritable.class,sr);
			 	int i=0;
			 	for (DoubleWritable doubleWritable : weightsArray) {
			 		weights[i]= doubleWritable.get();
			 		i++;
				}
			 	sr.close();
			 	/*while(sr.next(currkey)){
					sr.getCurrentValue(currValue);
					weights[currkey.get()] = currValue.get();
			 		System.out.println("In hessian mapper Grad key:: "+currkey.get()+". Grad Val:: "+currValue.get());
			 	}*/
			 	
			 	controlFlag1 = false;//reset the flag so that the common weights are not read in again and again
			}else if(controlFlag.equals("true") && controlFlag1){//this is the first iteration all weights must be zero
				 System.out.println("In hessian mapper. looks like this is the first iteration");

				weights = new double[phi_n.length];
				Arrays.fill(weights,0);
				controlFlag1 = false;//we have inited the weights once dont want to do the same thing again...
			}
			System.out.println("In hessian mapper# "+key);
			for(int i=0 ;i < phi_n.length ;i++){
				phi_n[i] = Double.parseDouble(featureTargetArray[i]);
				System.out.print("VAL"+i+"::"+phi_n[i]+"   ");
			}
			 System.out.println("*************inited running the lr on one daya point*********");
			
			 //Great we avoided the need for a huge temporary matrix completely!. We just need to collect the 
			 //H(i,j) directly in the for loop below. This saves RAM. Even for a 50*50 double hessian assuming 
			 // it takes 2500*8 = 20000 =20KB of temp ram memory/mapper. The O/P space on the HDFS will be the same.
			 Double constant =LRUtil.efficientHessianConstant(weights, phi_n);
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
			 System.out.println("Exiting Hessian mapper# "+key);
			 System.out.println();			 
		}
		
	}
		
}

