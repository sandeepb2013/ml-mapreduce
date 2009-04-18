package mapreduce.reducer;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.mlwritables.DoubleArrayWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TestSandboxReducer {
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleArrayWritable, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key,
				Iterator<DoubleArrayWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
				Writable[] ars =null;
				DoubleWritable[] array=null;
				while(values.hasNext()){//add ea
					 ars = values.next().get();
					 array = new DoubleWritable[ars.length];
					 for (int i = 0; i < array.length; i++) {
						 array[i] = (DoubleWritable)ars[i];
						 output.collect(key, new Text(String.valueOf(array[i].get())));
					}
					 
					 
				}
				
				
				
			
		}
	
	}
}
