package mapreduce.reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import mapreduce.mlwritables.DoubleArrayWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LRGradientReducer {
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleWritable> {
	 	
	 private JobConf conf;
	   private static double[] weights=null;//universal model
	   private boolean controlFlag1=true;
	   
	   public void configure(JobConf conf) {
		    
				this.conf=conf;
				conf.setOutputValueClass(Text.class);
			
	   }
		
		
	public void reduce(IntWritable key, Iterator<DoubleArrayWritable> values,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
			throws IOException  {
			System.out.println("In Gradient reducer# "+key);
			
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
			
				//add it to the sum
				for(int i = 0 ;i <firstStageElements.length;i++){//add the values to the sum 
					System.out.print(firstStageSum[i]+", "+firstStageElements[i]+" :: ");
					firstStageSum[i]= new DoubleWritable(firstStageSum[i].get() +(double)((DoubleWritable)firstStageElements[i]).get());
					
				}System.out.println("~~~");
			}
			List ls = Arrays.asList(firstStageSum);//op the list as text
			Text s = new Text(ls.toString());
			System.out.println("Gradient Reduce O/p:"+ls.toString());
				
			for(int i = 0 ;i < firstStageSum.length;i++){
				try{	
					//System.out.println("In Gradient reducer writing value: "+String.valueOf(firstStageSum[i].get()));
					output.collect(new IntWritable(i),firstStageSum[i]);//Write o/p to the file.
				
				}catch(Exception e){
					 System.out.println("In gradient reducer Exception while writing to the collector."+" value :"+new Text(String.valueOf(firstStageSum[i].get())));
					 System.out.println("In gradient reducer. Value cal "+conf.getOutputValueClass());
					 e.printStackTrace();
				}
			}
			System.out.println("exiting Gradient reducer# "+key);
		
	}
}
}
