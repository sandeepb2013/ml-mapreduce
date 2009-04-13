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

import ml.algorithms.utils.*;
import mapreduce.utils.*;
/**
 * This class should read in the O/P of the hessian and calculate its inverse.
 * Once that is done it should Take the gradient file and just put in the 
 * */
public class LRChainedMap extends MapReduceBase implements Mapper<Text, DoubleWritable, Text, Text> {
	private double[] gradient =null;
    private double[][] hessian=null;
    private static int size=0;//stores the size of the gradient vector or equally the model coefficients size.
    private static int count=0;
    private static JobConf conf;
    
	 public void configure(JobConf conf) {
		   //now we need to read in the sequence file that contains the gradient information
		 try {
			 	FileSystem fs = FileSystem.get(conf);
			 	this.conf = conf;
			 	TreeMap<Integer,Double> gradienttemp =new TreeMap<Integer,Double>();
			 	//"hdfs://localhost:9000/user/hadoop/LRModel/gradient/part-00000"
			 	System.out.println("CONF IS:"+conf.get("whoami"));//Shows chainedMapConf in logs... But I am 
			 	String gradientFilePath = conf.get("gradientOP",null );//get the path
			 	if(gradientFilePath==null){
			 		System.err.println("Gradient file not found!");
			 		throw new IOException();
			 	}
			 	
			 	System.out.println("IN CHAIN MAPPER gradientFilePath"+gradientFilePath);
			 	SequenceFile.Reader sr = new SequenceFile.Reader(fs,new Path(gradientFilePath),new JobConf(false) );
			 	System.out.println("IN CHAIN MAPPER Created seq reader");
				IntWritable currkey =  new IntWritable(); 
				Text value = new Text();
				
				while(sr.next(currkey)){
					sr.getCurrentValue(value);
					gradienttemp.put(currkey.get(),Double.parseDouble(value.toString()));//get the model...
					System.out.println("Grad key:: "+currkey.get()+". Grad Val:: "+Double.parseDouble(value.toString()));
				}
				fs.delete(new Path(gradientFilePath), true);//make sure you delete the gradient
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
	
	public void map(Text key, DoubleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			count++;
			
			String[] indices= key.toString().split(" ");
			hessian[Integer.parseInt(indices[0].trim())][Integer.parseInt(indices[1].trim())] = value.get();
			System.out.println("InChainedMap Class count var = "+count+" Indices:: "+ indices[0]+", "+indices[1]);
			System.out.println("hessian val:"+hessian[Integer.parseInt(indices[0].trim())][Integer.parseInt(indices[1].trim())]);
			if(count==size*size){
				System.out.println("Recieved all the values for hessian!!!!!");
				System.out.println("Hessian size(N*N): "+hessian[gradient.length-1].length);
				System.out.println("CONF IS:"+conf.get("whoami"));
				try {
				LinearAlgebraUtils lra = new LinearAlgebraUtils();
				double[][] inverse =lra.Inverse(hessian);
				double[] update = new double[gradient.length];
				double tempSum=0;
				for(int i=0;i<inverse[gradient.length-1].length;i++){
					tempSum=0;
					for(int j=0;j<inverse[gradient.length-1].length;j++){
						tempSum+= inverse[i][j]* gradient[j];//M*1 matrix results. Where M is # of features
					}update[i]=tempSum;
					 System.out.println();
					
				}
				/**
				 * Fetch the cached model to update.
				 * **/
				System.out.println("Fetching the cached model file ");
				Path[] files = FileUtils.getCachedFiles(conf);
				BufferedReader wordReader = null; 
				String cachedPath=null;
				for (int j = 0; j < files.length; j++) {
					if(FileUtils.getResouceAsReader(files[j], conf)!=null){
						cachedPath = files[j].getName();
						break;
					}
						
				}if(cachedPath!=null)
					wordReader = new BufferedReader(new FileReader(cachedPath.toString()));
				else
					throw new FileNotFoundException();
				/**
				 * Now read in the model
				 * */
				System.out.println("Fetching the parameters from the cached files ");
				double[] w = new double[size];
				String line = wordReader.readLine();
				 while(line!=null){
					 String[] split = line.split(" ");
					 w[count]= Double.parseDouble(split[1].trim());//Read in the model;
					 count++;
					 line = wordReader.readLine();
				 }
				 wordReader.close();
				 System.out.println("Writing to the cached model now");
				 BufferedWriter bwr = new BufferedWriter(new FileWriter(cachedPath));
				 for(int i=0;i<w.length;i++){
					 w[i]-=update[i];//update the model and write it to file. 
					 bwr.write(i+" "+w[i]+"\n");
				 }
				 bwr.close();
				 System.out.println("Exiting the chained mapper....");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		//now we willtry to get the hessian and get its inverse
		
		
		
		
	} 
	  
}
