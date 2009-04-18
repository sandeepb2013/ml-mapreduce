package mapreduce.mapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;

import javax.sound.midi.Sequence;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import ml.algorithms.utils.*;
import mapreduce.utils.*;
/**
 * This class should read in the O/P of the hessian and calculate its inverse.
 * Once that is done it should Take the gradient file and just put in the 
 * */
public class LRChainedMap extends MapReduceBase implements Mapper<Text, DoubleWritable, IntWritable, DoubleWritable> {
	private double[] gradient =null;
    private double[][] hessian=null;
    private static int size=0;//stores the size of the gradient vector or equally the model coefficients size.
    private static int count=0;
    private static JobConf conf;
    private MultipleOutputs mos;

	 public void configure(JobConf conf) {
		   //now we need to read in the sequence file that contains the gradient information
		 try {
			 	
			 	this.conf = conf;
			 	mos = new MultipleOutputs(conf);
			 	//read in the gradient file
			 	TreeMap<Integer,Double> gradienttemp =new TreeMap<Integer,Double>();
			 	System.out.println("In chained Mapper conf is:"+conf.get("whoami"));//Shows chainedMapConf in logs... But I am 
			 	String gradientFilePath = conf.get("gradientFile",null );//get the path
			 	System.out.println("In chained Mapper The Gradient file directory for Chain mapper is: "+gradientFilePath);
			 	System.out.println("In chained Mapper The weights file directory for Chain mapper is: "+conf.get("weightsFile",null ));//get the path);
			 	FileUtils.getSequenceReader(gradientFilePath, conf);
			 	SequenceFile.Reader sr = FileUtils.getSequenceReader(gradientFilePath, conf);//new SequenceFile.Reader(fs,new Path(gradientFilePath),new JobConf(false) );
				IntWritable currkey =  new IntWritable(); 
				DoubleWritable value = new DoubleWritable();
				
				while(sr.next(currkey,value)){
					gradienttemp.put(currkey.get(),value.get());//get the model...
					System.out.println("Grad key:: "+currkey.get()+". Grad Val:: "+Double.parseDouble(value.toString()));
				}
				FileUtils.deleteFileAtURI(gradientFilePath, conf);//we dont need the gradient file, it will be produced again in the gradient reducer
				//fs.delete(new Path(gradientFilePath), true);//make sure you delete the gradient
				hessian= new double[gradienttemp.size()][gradienttemp.size()];
				size=gradienttemp.size();
				gradient = new double[size];
				for(int i =0 ;i<size;i++){
					gradient[i] = gradienttemp.get(i);
				}
				System.out.println("Size= "+size);
				
			
		} catch (IOException e) {
			 System.out.println("EXCEPTION!!!!"+e.getMessage());
			 e.printStackTrace();
		}
		   
	}
	/*
	 * The only out put of the chain mapper is the parameters. They are stored in a file. The chain mapper also has the responsibility
	 * of deleting the output directory where the gradient is stored. We may want to move the clean up to a seperate module to encapsulate it
	 * better in later designs...
	 * */
	public void map(Text key, DoubleWritable value,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
			throws IOException {
			count++;
			double[] weightsTemp=null;
			String[] indices= key.toString().split(" ");
			//gather the hessian
			hessian[Integer.parseInt(indices[0].trim())][Integer.parseInt(indices[1].trim())] = value.get();
			
			System.out.println("InChainedMap Class count var = "+count+" Indices:: "+ indices[0]+", "+indices[1]);
			System.out.println("hessian val:"+hessian[Integer.parseInt(indices[0].trim())][Integer.parseInt(indices[1].trim())]);
			//if the hessian has been gathered start the update to the parameters.
			if(count==size*size){
				System.out.println("In chained Mapper Recieved all the values for hessian!!!!!");
				System.out.println("In chained Mapper Hessian size(N*N): "+hessian[gradient.length-1].length);
				System.out .println("In chained Mapper CONF IS:"+conf.get("whoami"));
				LinearAlgebraUtils lra = new LinearAlgebraUtils();
				double[][] inverse;
				String controlFlag = conf.get("controlFlag1",null );//get the path
				System.out .println("In chained Mapper control flag is :"+conf.get("controlFlag1",null ));
				
				//Read in the weights file and subtract the updates from that.
				if(controlFlag.equals("false")){//Read weights only if its not the first iteration
					weightsTemp = new double[hessian[Integer.parseInt(indices[0].trim())].length];//if its anything but the first iteration
					String weightsFile = conf.get("weightsFile",null );//get the path
					IntWritable currkey =  new IntWritable(); 
				 	SequenceFile.Reader sr = FileUtils.getSequenceReaderFromFile(weightsFile, conf);
				 	DoubleWritable currValue = new DoubleWritable();
				 	while(sr.next(currkey)){
						sr.getCurrentValue(currValue);
						weightsTemp[currkey.get()]= currValue.get();//get the model...
						System.out.println("In chained mapper. Weight key:: "+currkey.get()+". Weight Val:: "+currValue.get());
					}
				 	sr.close();
					
				}else if(controlFlag.equals("true") ){//this is the first iteration all weights must be zero
					 System.out.println("In gradient mapper. looks like this is the first iteration");
					 weightsTemp = new double[hessian[Integer.parseInt(indices[0].trim())].length];
					 Arrays.fill(weightsTemp, 0);
					 
				 }
				
				try {
					inverse = lra.Inverse(hessian);
				    double[] update = new double[gradient.length];
					double tempSum=0;
					String weightsFile = conf.get("weightsFile",null );//get the path
					SequenceFile.Writer swrt = FileUtils.getSequenceWriter(weightsFile,IntWritable.class, DoubleWritable.class,conf);
					for(int i=0;i<inverse[gradient.length-1].length;i++){
						tempSum=0;
						for(int j=0;j<inverse[gradient.length-1].length;j++){
							tempSum+= inverse[i][j]* gradient[j];//M*1 matrix results. Where M is # of features
						}update[i]=tempSum;
						System.out.println(" "+(tempSum));
						controlFlag = conf.get("controlFlag1",null );//get the path
						String controlFlag2 = conf.get("controlFlag2",null );//get the path
						
						
						if(controlFlag.equals("true")){
							//first iteration just write the weights to O/p as such
							System.out.println("In chained reducer(First Iteration). Collecting O/P from primary collector. Value: "+(update[i]));
							swrt.append(new IntWritable(i), new DoubleWritable(update[i]));
							output.collect(new IntWritable(i), new DoubleWritable(update[i]));
						}else if((controlFlag.equals("false"))){
							System.out.println("In chained reducer(> 1st iteration). Collecting O/P from primary collector. Value: "+(weightsTemp[i]-update[i]));
							
							 swrt.append(new IntWritable(i), new DoubleWritable(weightsTemp[i]-update[i]));
							 output.collect(new IntWritable(i), new DoubleWritable(weightsTemp[i]-update[i]));
							
						}else if((controlFlag2.equals("last"))){
							//Write The weights file as an additional O/P
							System.out.println("In chained reducer. Collecting O/P from alternate collector. Value: "+(weightsTemp[i]-update[i]));
							
							swrt.append(new IntWritable(i), new DoubleWritable(weightsTemp[i]-update[i]));
							mos.getCollector("seq", reporter).collect(new IntWritable(i), new DoubleWritable(weightsTemp[i]-update[i]));
						}
					}swrt.close();
				 }catch (Exception e) {
					e.printStackTrace();
				}
			}
			
	} 
}
	  
