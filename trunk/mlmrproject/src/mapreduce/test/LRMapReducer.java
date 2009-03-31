package mapreduce.test;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 * The Map reduce class responsible for parallelizing Logistic Regression algorithms
 * 
 * */
public class LRMapReducer {

	private static LRUtils lrUtils;
    private static final String HDFS_STOPWORD_LIST = "/data/stop_words.txt";
    
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> { 
	   private Path[] model;
	       
	   public void configure(JobConf conf) {
		    try {
		      String stopwordCacheName = new Path(HDFS_STOPWORD_LIST).getName();
		      Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		      if (null != cacheFiles && cacheFiles.length > 0) {
		        for (Path cachePath : cacheFiles) {
		         System.out.println("cachePath::"+cachePath);
		          }
		        }else
		        	System.out.println("BAD BAD CACHE!");
		    } catch (IOException ioe) {
		      System.err.println("IOException reading from distributed cache");
		      System.err.println(ioe.toString());
		    }
		  }

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			 String phi_n_t = value.toString();
			 String[] phi_n_str= phi_n_t.split(" ");//break into tokens
			 double[] phi_n = new double[phi_n_str.length];
			 for(int i=0 ;i < phi_n_str.length ;i++){
				 phi_n[i] = Double.parseDouble(phi_n_str[i]);
				 System.out.print(phi_n[i]);
			 } System.out.println();
			 
			// lrUtils.calculateGradient(w, t, phi_n)
		}
	}
		public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
			 	    
			public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	 	       int sum = 0;
	 	       while (values.hasNext()) {
	 	         sum += values.next().get();
	 	       }
		       //output.collect(key, new IntWritable(sum));
			}
		}

	}

