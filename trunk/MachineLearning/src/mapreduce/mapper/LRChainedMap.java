package mapreduce.mapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;


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

			 	TreeMap<Integer,Double> gradienttemp =new TreeMap<Integer,Double>();
			 	System.out.println("CONF IS:"+conf.get("whoami"));//Shows chainedMapConf in logs... But I am 
			 	String gradientFilePath = conf.get("gradientFile",null );//get the path
			 	System.out.println("The Gradient file path for Chain mapper is: "+gradientFilePath);
			 	System.out.println("The weights file path for Chain mapper is: "+conf.get("weightsFile",null ));//get the path);
			 	FileUtils.getSequenceReader(gradientFilePath, conf);
			 	System.out.println("gradientFilePath: "+gradientFilePath);
			 	SequenceFile.Reader sr = FileUtils.getSequenceReader(gradientFilePath, conf);//new SequenceFile.Reader(fs,new Path(gradientFilePath),new JobConf(false) );
				IntWritable currkey =  new IntWritable(); 
				Text value = new Text();
				
				while(sr.next(currkey)){
					sr.getCurrentValue(value);
					gradienttemp.put(currkey.get(),Double.parseDouble(value.toString()));//get the model...
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
			
			//System.out.println("InChainedMap Class count var = "+count+" Indices:: "+ indices[0]+", "+indices[1]);
			//System.out.println("hessian val:"+hessian[Integer.parseInt(indices[0].trim())][Integer.parseInt(indices[1].trim())]);
			//if the hessian has been gathered start the update to the parameters.
			if(count==size*size){
				System.out.println("Recieved all the values for hessian!!!!!");
				System.out.println("Hessian size(N*N): "+hessian[gradient.length-1].length);
				System.out.println("CONF IS:"+conf.get("whoami"));
				LinearAlgebraUtils lra = new LinearAlgebraUtils();
				double[][] inverse;
				String controlFlag = conf.get("controlFlag1",null );//get the path
				//Read in the weights file and subtract the updates from that. Delete the old weights file and rewrite it.
				if(controlFlag.equals("false")){//gather weights if its not true.
					weightsTemp = new double[hessian[Integer.parseInt(indices[0].trim())].length];//if its anything but the first iteration
					String weightsFile = conf.get("weightsFile",null );//get the path
					IntWritable currkey =  new IntWritable(); 
				 	SequenceFile.Reader sr = FileUtils.getSequenceReader(weightsFile, conf);
				 	DoubleWritable currValue = new DoubleWritable();
				 	while(sr.next(currkey)){
						sr.getCurrentValue(currValue);
						weightsTemp[currkey.get()]= currValue.get();//get the model...
						System.out.println("Weight key:: "+currkey.get()+". Weight Val:: "+currValue.get());
					}
					FileUtils.deleteFileAtURI(weightsFile, conf);
				}
				
				try {
					inverse = lra.Inverse(hessian);
				    double[] update = new double[gradient.length];
					double tempSum=0;
					for(int i=0;i<inverse[gradient.length-1].length;i++){
						tempSum=0;
						for(int j=0;j<inverse[gradient.length-1].length;j++){
							tempSum+= inverse[i][j]* gradient[j];//M*1 matrix results. Where M is # of features
						}update[i]=tempSum;
						controlFlag = conf.get("controlFlag1",null );//get the path
						if(controlFlag.equals("true")){
							//first iteration just write the weights to O/p as such
							output.collect(new IntWritable(i), new DoubleWritable(update[i]));
						}else if((controlFlag.equals("false"))){
							 output.collect(new IntWritable(i), new DoubleWritable(weightsTemp[i]-update[i]));
							
						}else if((controlFlag.equals("last"))){
							//Write The weights file as an additional
							mos.getCollector("seq", reporter).collect(new IntWritable(i), new DoubleWritable(weightsTemp[i]-update[i]));
						}
					}
				 }catch (Exception e) {
					e.printStackTrace();
				}
			}
			
	} 
}
	  
