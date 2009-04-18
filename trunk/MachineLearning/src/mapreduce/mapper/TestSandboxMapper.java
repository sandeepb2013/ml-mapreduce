package mapreduce.mapper;

import java.io.IOException;

import mapreduce.mlwritables.DoubleArrayWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestSandboxMapper {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> { 
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter) throws IOException {
			 DoubleWritable[] update = new DoubleWritable[10];
			for(int i=0;i<10;i++){
				update[i]= new DoubleWritable(i);
			}
			 System.out.println("In mapper");
			 DoubleArrayWritable ars = new DoubleArrayWritable();
			 ars.set(update);
			 try{
			 output.collect(new IntWritable(1),ars);
			
		     }catch(Exception e){
		    	 System.out.println("In mapper");
		     }
	
		}
	}
}
