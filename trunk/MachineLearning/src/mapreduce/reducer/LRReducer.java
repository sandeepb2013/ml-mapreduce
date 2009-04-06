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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LRReducer {
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleArrayWritable, IntWritable, Text> {
 	    
		@Override
		public void reduce(IntWritable key, Iterator<DoubleArrayWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException  {
				System.out.println("In reducer# "+key);
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
					for(int j=0;j<firstStageElements.length;j++){
						System.out.print("VALR: "+j+" :"+firstStageElements[j]+"  ");
					}System.out.println();
					System.out.println(" firstStageElements.length: "+firstStageElements.length+" firstStageSum.size: "+firstStageSum.length);
					//add it to the sum
					for(int i = 0 ;i <firstStageElements.length;i++){
						firstStageSum[i]= new DoubleWritable(firstStageSum[i].get() +(double)((DoubleWritable)firstStageElements[i]).get());
						
					}
					for(int j=0;j<firstStageSum.length;j++){
						System.out.print("VALF: "+j+" :"+firstStageSum[j]+"  ");
					}System.out.println();
					
				}
				List ls = Arrays.asList(firstStageSum);
				Text s = new Text(ls.toString());
				System.out.println("Reduce O/p:"+ls.toString());
				output.collect(key, s);//Write o/p to the file.
				System.out.println("exiting reducer# "+key);
			
		}
	}
}
