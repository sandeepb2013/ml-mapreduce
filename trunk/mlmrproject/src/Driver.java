import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import mapreduce.test.*;
//user supplies i/p file dir and o/p dir
public class Driver {

	public static String HDFS_LR_MODEL="model.dat";
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(Driver.class);
		//LRMapReducer lrmapreduce = new LRMapReducer(LRUtils.class);
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(conf, new Path(args[0]));//.setInputPath(conf,new Path("src"));
		FileOutputFormat.setOutputPath(conf ,new Path(args[1]));//Make default model in args[1]
		//First create an empty model file in the O/P location of the HDFS
		FileSystem fs=null;
		Path hdfsPath  =null;
		try {
			fs = FileSystem.get(conf);//fs object will give you access to the FS 
			System.out.println("HD: "+fs.getHomeDirectory()+" WD: "+fs.getWorkingDirectory()+" URI: "+fs.getUri());
		if(args[1].charAt(args[1].length()-1) =='/')
			args[1] = args[1].substring(0,args[1].length()-1);
		hdfsPath = new Path("/FinalLRModel/model.txt");//this is the relative path to the fs
	    File f =new File("model.txt");
	    String path = f.getAbsolutePath();
	    if(!f.exists() && !f.createNewFile()){
	    	System.out.println("Could not create model file in "+path+ " Check your permissions");
	    	return;
	    }
	    fs.copyFromLocalFile(true, true, new Path(path),hdfsPath);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    System.out.println("Creating a file in "+fs.getHomeDirectory());
	    System.out.println("HDFSPATH: "+hdfsPath.toUri());
		try {
			DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		conf.setMapperClass(mapreduce.test.LRMapReducer.Map.class);

		conf.setReducerClass(mapreduce.test.LRMapReducer.Reduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
