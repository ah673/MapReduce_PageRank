import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class PageRank {

    public static void main(String[] args) throws IOException {
	int numRepititions = 5; //The number of PageRank passes to run
	long leftover = 0; //How much PageRank mass did not get moved to any node
	long size = 0; // The size of the internet graph
	for(int i = 0; i < 2*numRepititions; i++) { //We need to run 2 iterations to make 1 pass
	    Job job;
	    //Run the right job for the current pass
	    if(i%2 == 0) {
	    	System.out.println("getting Trust job");
	    	System.out.println("I MOD 2 : ======== leftover" + leftover + " " + "size" + size);
	    	job = getTrustJob();
	    }
	    else {
	    	System.out.println("getting Leftover job");
	    	System.out.println("leftover" + leftover + " " + "size" + size);
	    	job = getLeftoverJob(leftover, size);
	    }

	    String inputPath = i == 0 ? "input" : "stage" + (i-1);
	    String outputPath = "stage" + i;

	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));

	    try { 
		job.waitForCompletion(true); //run the job
	    } catch(Exception e) {
		System.err.println("ERROR IN JOB: " + e);
		return;
	    }
	    
	    Counters counters = job.getCounters(); 
	    Counter c1 = counters.findCounter(HadoopCounter.COUNTERS.NUM_OF_NODES); 
	    System.out.println(c1.getDisplayName() + " : " + c1.getValue());
	    
	    Counter leftoverPR = counters.findCounter(HadoopCounter.COUNTERS.LEFTOVER_PAGE_RANK); 
	    System.out.println(leftoverPR.getDisplayName() + " : " + leftoverPR.getValue()); 
	    
	    if(i%2 == 0) {
		// Set up leftover and size
	    	System.out.println("settingLeftover and size");
	    	size = c1.getValue(); 
	    	System.out.println("SIZE " + size);
	    	leftover = leftoverPR.getValue();
	    } else {
		// Set up leftover and size
	    	System.out.println("ELSE SIZE " + size);
	    	size = 0;  
	    	leftover = 0;
	    }
		
	}
    }
    public static Job getStandardJob(String l, String s) throws IOException {
	Configuration conf = new Configuration();
	if(!l.equals("") && !s.equals("")) { //if we're in the Leftover job case
	    conf.set("leftover", l);         //note that we need to do this here since we don't have access to the configuration elsewhere
	    conf.set("size", s);
	}
	Job job = new Job(conf);

	job.setOutputKeyClass(IntWritable.class); //We output <Int, Node> pairs
	job.setOutputValueClass(Node.class);

	job.setInputFormatClass(NodeInputFormat.class); //We take in <Int,Node> pairs
	job.setOutputFormatClass(NodeOutputFormat.class);

	job.setJarByClass(PageRank.class); //The current jar we're in

	return job;
    }

    public static Job getTrustJob() throws IOException{

	Job job = getStandardJob("", ""); //We don't need any extra variables

	job.setMapOutputKeyClass(IntWritable.class); //Our mapper puts out something different than our reducer
	job.setMapOutputValueClass(NodeOrDouble.class); //in particular, we output <Int, Node+Double> pairs
	
	job.setMapperClass(TrustMapper.class);
	job.setReducerClass(TrustReducer.class);

	return job;
    }

    public static Job getLeftoverJob(long l, long s) throws IOException{
    Job job = getStandardJob("" + l, "" + s);

	job.setMapperClass(LeftoverMapper.class);
	job.setReducerClass(LeftoverReducer.class);

	return job;
    }
}
	       

    


