import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import mapreduce.mlwritables.DoubleArrayWritable;
import ml.algorithms.utils.LinearAlgebraUtils;
import mapreduce.reducer.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.ChainReducer;


public class Driver2 {

	/**
	 * @param args
	 * fs = FileSystem.get(confHessian);//fs object will give you access to the Dist FS 
			System.out.println("HD: "+fs.getHomeDirectory()+" WD: "+fs.getWorkingDirectory()+" URI: "+fs.getUri());
	 */
	public static void main(String[] args) {
		
		JobControl jobControl =new JobControl("LR job");
		LRJob jobRunner= new LRJob(jobControl, Driver.class); 
		jobRunner.configureJob(args);//configure the jobs t be run
		Thread thr = new Thread(jobRunner);//
		thr.start();//Start the thread
		
		try {
			
			jobRunner.addJobs();
			for(int i=0;i<1;i++){
				while(!jobControl.allFinished()){
					Thread.sleep(500);//Sleep now until the last job is finished.
				}
			}jobRunner.stopJobControl();
			
					
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
	
	
	
}

class LRJob implements Runnable{

	private JobControl lrJob;//Wrapped job Control object
	private JobConf confHessian;
	private JobConf confGradient;
	private JobConf reduceConf;
	private JobConf chainedMapConf;//special configuration 
	private FileStatus[] gradientFiles;
	private FileStatus[] weightsFile;
	private boolean controlFlag1;//signals to the LR job that the first iteration is in motion.
	private Class mainClass;
	
	LRJob(JobControl jc, Class mainClass ){
		 lrJob=jc;
		 this.mainClass = mainClass.getClass();
		 controlFlag1= true;
	}
	public void run() {
		lrJob.run();
	}
	public void stopJobControl(){
		lrJob.stop();
	}
	/**
	 * Configure the job ,like below, before adding it to job control
	 * */
	public void  configureJob(String[] args){
		FileSystem fs=null;
		confHessian = new JobConf(mainClass.getClass());//Is in a chained configuration here.
		confGradient = new JobConf(mainClass.getClass());
		reduceConf = new JobConf(mainClass.getClass());
		chainedMapConf = new JobConf(mainClass.getClass());//special configuration 
		
		chainedMapConf.set("whoami", "chained MapConf");
		confHessian.set("whoami", "conf Hessian");
		confGradient.set("whoami", "conf Gradient");
		reduceConf.set("whoami", "reduce Conf");
		
		
		//Section sets o/p key and value classes. fIxed and hard coded for each implementation.	
		confHessian.setMapOutputKeyClass(Text.class);
		confHessian.setMapOutputValueClass(DoubleWritable.class);
		confGradient.setMapOutputKeyClass(IntWritable.class);
		confGradient.setMapOutputValueClass(DoubleArrayWritable.class);
		confGradient.setOutputKeyClass(IntWritable.class);
		confGradient.setOutputValueClass(Text.class);
		confGradient.setOutputFormat(SequenceFileOutputFormat.class);
		
		
		if(args[1].charAt(args[1].length()-1) =='/'){
			args[1] = args[1].substring(0,args[1].length()-1);
			}
	
		//start setting the O/P paths 
		FileInputFormat.setInputPaths(confHessian, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(confHessian ,new Path(args[1]+"/weights/"));//Final O/P should be the weights file
		FileInputFormat.setInputPaths(confGradient, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(confGradient ,new Path(args[1]+"/gradient/"));//will help in reading cleanly when we need it... 
		//FileOutputFormat.setOutputPath(chainedMapConf ,new Path(args[1]+"/temp/"));//tempvariable looks like the chain mapper needs an O/P
		//chainedMapConf.addResource(SequenceFileOutputFormat.getOutputPath(confGradient));
		
		confHessian.setMapperClass(mapreduce.mapper.LRHessianMapper.Map.class);//need to be replaced by multiple inputs
		confHessian.setReducerClass(mapreduce.reducer.LRHessianReducer.Reduce.class);//
		confHessian.setCombinerClass(mapreduce.reducer.LRHessianReducer.Reduce.class);
		confGradient.setMapperClass(mapreduce.mapper.LRGradientsMapper.Map.class);//need to be replaced by multiple inputs
		confGradient.setReducerClass(mapreduce.reducer.LRGradientReducer.Reduce.class);//
		
		try {
			fs = FileSystem.get(confGradient);
			Path outputPaths= FileOutputFormat.getOutputPath(confGradient);//get the O/p paths from the gradient
			gradientFiles = fs.listStatus(outputPaths);
			outputPaths = FileOutputFormat.getOutputPath(confHessian);
			weightsFile = fs.listStatus(outputPaths);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		/*
		 * Configure the I/P paths 
		 * */
		chainedMapConf.set("gradientFile", gradientFiles[1].getPath().toString());//The gradient is needed by the chain reducer.
		confGradient.set("weightsFile", weightsFile[1].getPath().toString());
		confHessian.set("weightsFile", weightsFile[1].getPath().toString());
		
		/*Set the control flag. Signals to the jobs that the first iteration is happening.
		 * Basically if its the first iteration the job will subtract the producty of the hessian*gradient from null or 0 weights.
		 * After the second iteration the 
		 * */
		chainedMapConf.set("controlFlag1", String.valueOf(controlFlag1));
		confGradient.set("controlFlag1", String.valueOf(controlFlag1));
		confHessian.set("controlFlag1", String.valueOf(controlFlag1));
		if(controlFlag1){
			controlFlag1=false;//turn off after the first batch is done...
		}
		/*The first parameter is the main job's job conf. For the set reducer the reducerConf, has precedence over the job's JobConf. 
		 * This precedence is in effect when the task is running.
		 * IMPORTANT: There is no need to specify the output key/value classes for the ChainReducer, 
		 * this is done by the setReducer or the addMapper for the last element in the chain.  
		 * */
		ChainReducer.setReducer(confHessian,mapreduce.reducer.LRHessianReducer.Reduce.class, 
				Text.class, DoubleWritable.class,
				Text.class, DoubleWritable.class, true, reduceConf);//sets the O/P K,V pair for the chain reducer.
		ChainReducer.addMapper(confHessian, mapreduce.mapper.LRChainedMap.class, Text.class, 
				DoubleWritable.class, IntWritable.class, DoubleWritable.class, false, chainedMapConf);//set the O/P K,V pair for the chain mapper as well.
		
		
	}
	public void addJobs(){
		try {
			lrJob.addJob(new Job(confGradient));
			lrJob.addJob(new Job(confHessian));
			//lrJob.addJob(new Job(chainedMapConf));
			//lrJob.addJob(new Job(confHessian));
		} catch (IOException e) {
			System.out.println("Bad job added to job control!");
			e.printStackTrace();
		}
	}
}