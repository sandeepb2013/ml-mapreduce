package mlmr.mapred.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


import mlmr.utils.FileUtils;
import mlmr.utils.LRUtil;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/*
 * The Map reduce class responsible for parallelising Logistic Regression algorithms
 * 
 * */
public class LRHessianMapper {

	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> { 
	  
		   private JobConf conf;
		   private DoubleWritable[] weights =null;
		   
		   public void configure(JobConf conf) {
			   this.conf=conf;
			   System.out.println("Con figuring the hessian mapper..");
			   String controlFlag = conf.get("controlFlag1",null );//get the path
			   System.out.println("In Hessian mapper. The control flag is"+conf.get("controlFlag1",null ));

				if(controlFlag.equals("false")){
					System.out.println("In hessian mapper Getting the weights now");
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
					 System.out.println("In hessian mapper. looks like this is the first iteration");
					 weights = null;
					
				}
		  }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			 //by default the key is the byte offset of the value in the file.??
			String phi_n_t = value.toString();
			String[] featureTargetArray= phi_n_t.split(" ");//break into tokens
			DoubleWritable[] phi_n = new DoubleWritable[featureTargetArray.length-1];//one less because target is the last value.
			System.out.println("In hessian mapper# "+key);
			
			if(weights==null){
				weights = new DoubleWritable[phi_n.length];
			}
			System.out.print("Weights: ");
			for(int i=0 ;i < phi_n.length ;i++){
				phi_n[i] = new DoubleWritable(Double.parseDouble(featureTargetArray[i]));
				System.out.print("Val= "+i+": "+phi_n[i]+", ");
				if(weights[i]==null){
					weights[i]=new DoubleWritable(0);
					
				}
				System.out.print(weights[i]+", ");
			}System.out.println();
			
			 System.out.println("*************init done! running the lr on one data point*********");
			
			 //Great we avoided the need for a huge temporary matrix completely!. We just need to collect the 
			 //H(i,j) directly in the for loop below. This saves RAM. Even for a 50*50 double hessian assuming 
			 // it takes 2500*8 = 20000 =20KB of temp ram memory/mapper. The O/P space on the HDFS will be the same.
			 DoubleWritable constant =LRUtil.efficientHessianConstant(weights, phi_n);
			 System.out.println("Hessian Constant= "+constant);
			 
			  for(int i =0;i< phi_n.length;i++){
				 for(int j =0;j< phi_n.length;j++){
					 DoubleWritable temp =LRUtil.efficientHessianVariable(constant,phi_n, i, j);
					//all mappers collect the H(i,j) here. We will sum it up in the reducers
					 output.collect(new Text(i+" "+j),temp);
					 
					 
				 }
			 }
			
			 System.out.println("Exiting Hessian mapper# "+key);
			 System.out.println();			 
		}
		
	}
		
}

