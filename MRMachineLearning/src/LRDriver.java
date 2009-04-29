


import java.io.IOException;
import java.util.TreeMap;



import mlmr.mapred.mapper.LRChainedMapper;
import mlmr.mapred.mapper.LRGradientMapper;
import mlmr.mapred.mapper.LRHessianMapper;
import mlmr.mapred.reducer.LRGradientReducer;
import mlmr.mapred.reducer.LRHessianReducer;
import mlmr.mapred.writable.DoubleArrayWritable;
import mlmr.utils.FileUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.ChainReducer;

/*
 * Observations:
 * In the chain reducer. the output of the chained reducer is controlled
 * by the job conf of the main map-reduce class. Rather than the mapper in the chain reducer.
 * In our case the JobConf for the hessian is incharge of the O/P directories and not the chainedMap conf. 
 * */
public class LRDriver {

	/**
	 * @param args
	 * fs = FileSystem.get(confHessian);//fs object will give you access to the Dist FS 
			System.out.println("HD: "+fs.getHomeDirectory()+" WD: "+fs.getWorkingDirectory()+" URI: "+fs.getUri());
	 */
	public static void main(String[] args) {
		
		JobControl jobControl =new JobControl("LR job");
		int iterations =6;
		LRJob jobRunner= new LRJob(jobControl, LRDriver.class,  iterations); 
		//configure the jobs t be run
		
		
		try {
			
			
			//wait before the added job is completed
			for(int i=0; i<iterations;i++){
				System.out.println("Starting "+(i+1)+"th iteration");
				jobRunner.configureJob(args,i);
				jobRunner.runJobs();//add the job now and wait until it has finished
				
			}
			System.out.println("Job finished...");
					
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
	
	
	
}

class LRJob implements Runnable{

	private JobClient lrJob = new JobClient();//Wrapped job Control object
	private JobConf HessianJobConf;
	private JobConf GradientJobConf;
	private JobConf reduceConf;
	private JobConf chainedMapConf;//special configuration 
	
	//private FileStatus[] gradientFiles;
	//private FileStatus[] weightsFile;
	private String gradientFilesPath;
	private String weightsFilePath;
	private boolean controlFlag1;//signals to the LR job that the first iteration is in motion.
	private String mainClass;
	private int iterations=0;
	
	LRJob(JobControl jc, Class mainClass, int iterations ){
		 this.iterations = iterations;
		 //lrJob=jc;
		 this.mainClass = mainClass.getName();
		 //System.out.println(this.mainClass.getName()+" : "+mainClass.getName());
		 controlFlag1= true;
	}
	public void run() {
		
	}
	public void stopJobControl(){
		//lrJob.stop();
	}
	/**
	 * Configure the job ,like below, before adding it to job control
	 * */
	public void  configureJob(String[] args, int i){
		FileSystem fs=null;
		HessianJobConf = new JobConf(mainClass);//Is in a chained configuration here.
		GradientJobConf = new JobConf(mainClass);
		reduceConf = new JobConf(mainClass);
		chainedMapConf = new JobConf(mainClass);//special configuration 
		
		chainedMapConf.set("whoami", "chained MapConf");
		HessianJobConf.set("whoami", "conf Hessian");
		GradientJobConf.set("whoami", "conf Gradient");
		reduceConf.set("whoami", "reduce Conf");
		
		
		//Section sets o/p key and value classes. fIxed and hard coded for each implementation.	
		HessianJobConf.setMapOutputKeyClass(Text.class);
		HessianJobConf.setMapOutputValueClass(DoubleWritable.class);
		
		GradientJobConf.setMapOutputKeyClass(IntWritable.class);
		GradientJobConf.setMapOutputValueClass(DoubleArrayWritable.class);
		GradientJobConf.setOutputKeyClass(IntWritable.class);
		GradientJobConf.setOutputValueClass(DoubleWritable.class);
		System.out.println("GradientJobConf.getOutputValueClass()"+GradientJobConf.getOutputValueClass());
		
		GradientJobConf.setOutputFormat(SequenceFileOutputFormat.class);

		//chainedMapConf.setOutputFormat(SequenceFileOutputFormat.class);
		HessianJobConf.setOutputFormat(SequenceFileOutputFormat.class);
		if(args[1].charAt(args[1].length()-1) =='/'){
			args[1] = args[1].substring(0,args[1].length()-1);
			}
	
		//start setting the O/P paths 
		FileInputFormat.setInputPaths(HessianJobConf, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(HessianJobConf ,new Path(args[1]+"/hessian/"));//Final O/P should be the weights file
		FileInputFormat.setInputPaths(GradientJobConf, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(GradientJobConf ,new Path(args[1]+"/gradient/"));//will help in reading cleanly when we need it... 
		//Looks like the OutputFormat for the chained mapper need not be set
		//SequenceFileOutputFormat.setOutputPath(chainedMapConf ,new Path(args[1]+"/weights/"));//tempvariable looks like the chain mapper needs an O/P
		//chainedMapConf.addResource(SequenceFileOutputFormat.getOutputPath(GradientJobConf));
		
		HessianJobConf.setMapperClass(LRHessianMapper.Map.class);//need to be replaced by multiple inputs
		HessianJobConf.setReducerClass(LRHessianReducer.Reduce.class);//
		HessianJobConf.setCombinerClass(LRHessianReducer.Reduce.class);
		GradientJobConf.setMapperClass(LRGradientMapper.Map.class);//need to be replaced by multiple inputs
		GradientJobConf.setReducerClass(LRGradientReducer.Reduce.class);//
		try {
			fs = FileSystem.get(GradientJobConf);
			Path outputPaths= FileOutputFormat.getOutputPath(GradientJobConf);//get the O/p paths from the gradient
			
			gradientFilesPath = outputPaths.toString();//fs.listStatus(outputPaths);//op path of the gradient
			//Universal model!
			Path path = new Path("/user/hadoop/model.dat");
			weightsFilePath = path.makeQualified(fs).toString();//fs.listStatus(outputPaths);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		/*
		 * Configure the I/P paths 
		 * */
		//chainedMapConf.set("gradientFile", gradientFiles[1].getPath().toString());//The gradient is needed by the chain reducer.
		//GradientJobConf.set("weightsFile", weightsFile[1].getPath().toString());
		//HessianJobConf.set("weightsFile", weightsFile[1].getPath().toString());
		
		chainedMapConf.set("gradientFile", gradientFilesPath);//The gradient is needed by the chain reducer.
		
		chainedMapConf.set("weightsFile", weightsFilePath);//The gradient is needed by the chain reducer.
		GradientJobConf.set("weightsFile",weightsFilePath);
		HessianJobConf.set("weightsFile", weightsFilePath);
		HessianJobConf.setNumReduceTasks(1);
		/*Set the control flag. Signals to the jobs that the first iteration is happening.
		 * Basically if its the first iteration the job will subtract the producty of the hessian*gradient from null or 0 weights.
		 * After the second iteration the 
		 * */
		
		System.out.println("i:: "+i+" Total iterations"+iterations);
		/*if(i ==iterations-1){
			chainedMapConf.set("controlFlag2", "last");
			System.out.println("Last iteration setting the redirect");
		}*/
		chainedMapConf.set("controlFlag1", String.valueOf(controlFlag1));
		GradientJobConf.set("controlFlag1", String.valueOf(controlFlag1));
		HessianJobConf.set("controlFlag1", String.valueOf(controlFlag1));
		if(controlFlag1){
			controlFlag1=false;//turn off after the first batch is done...
		}
		/*The first parameter is the main job's job conf. For the set reducer the reducerConf, has precedence over the job's JobConf. 
		 * This precedence is in effect when the task is running.
		 * IMPORTANT: There is no need to specify the output key/value classes for the ChainReducer, 
		 * this is done by the setReducer or the addMapper for the last element in the chain.  
		 * */
		//sets the O/P K,V pair for the chain reducer.
		ChainReducer.setReducer(HessianJobConf,LRHessianReducer.Reduce.class, 
				Text.class, DoubleWritable.class,
				Text.class, DoubleWritable.class, true, reduceConf);
		ChainReducer.addMapper(HessianJobConf, LRChainedMapper.class, Text.class, 
				DoubleWritable.class, IntWritable.class, DoubleWritable.class, false, chainedMapConf);//set the O/P K,V pair for the chain mapper as well.
		 /*MultipleOutputs.addMultiNamedOutput(HessianJobConf, "seq",
				   FileOutputFormat.class,
				   IntWritable.class, DoubleWritable.class);*/
		try {
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	public void runJobs(){
		try {
			Job dependency = new Job(GradientJobConf);
			Job dependent = new Job(HessianJobConf);
			dependent.addDependingJob(dependency);
			//lrJob.submitJob(GradientJobConf);
			GradientJobConf.setJar("/home/hadoop/Desktop/LRTest_new.jar");
			JobClient.runJob(GradientJobConf);
			System.out.println("Running the hessian job");
			GradientJobConf.setClass("DoubleArrayWritable",DoubleArrayWritable.class, DoubleArrayWritable.class);
			HessianJobConf.setJar("/home/hadoop/Desktop/LRTest_new.jar");
			HessianJobConf.setClass("DoubleArrayWritable",DoubleArrayWritable.class, DoubleArrayWritable.class);

			JobClient.runJob(HessianJobConf);
			//Clean up...
			FileSystem fs = FileSystem.get(GradientJobConf);
			Path outputPaths= FileOutputFormat.getOutputPath(GradientJobConf);
			fs.delete(outputPaths, true);
			outputPaths= FileOutputFormat.getOutputPath(HessianJobConf);
			fs.delete(outputPaths, true);
			fs.close();
			Path path = new Path("/user/hadoop/model.dat");
			weightsFilePath = path.makeQualified(fs).toString();
			SequenceFile.Reader swrt = FileUtils.getSequenceReaderFromFile(weightsFilePath, GradientJobConf);
			DoubleWritable[] weights =FileUtils.readIndexedSequenceFiles(GradientJobConf, DoubleWritable.class, swrt);
			for(int i= 0;i< weights.length;i++){
				System.out.println("Weight "+i+" = "+weights[i].get());
			}
			swrt.close();
			
		} catch (IOException e) {
			System.out.println("Bad job added to job control!");
			e.printStackTrace();
		}
	}
	
}
