package mapreduce.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/*a Simple class to just add all the elements that comprise of a single hessian.
 *  
 * */
public class LRHessianReducer {
	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
 	    
		
		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException  {
				System.out.println("In hessian reducer# "+key);
				DoubleWritable hessianElement=new DoubleWritable(0);
				while(values.hasNext()){//add each of the values of firstStageElements to firststagesum
					hessianElement.set(hessianElement.get()+values.next().get());//gathet 
				}System.out.println("The hessian sum for "+key+" is "+hessianElement.get());
				output.collect(key, hessianElement);			
		}
	}
}
