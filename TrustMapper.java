import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {
	//Implement your version of the map here.
    	System.out.println("======= TRUST MAPPING ==========");
    	System.out.println(key);
    	System.out.println(value);
    	
    	// First output : Pass along (nodeID, Node)
    	context.write(key, new NodeOrDouble(value));
    	
    	// Second output : Pass along Pagerank
    	int[] outgoingLinks = value.outgoing; 
    	for (int nodeID : outgoingLinks){
    		// construct output
    		IntWritable outgoingID = new IntWritable(nodeID);
    		NodeOrDouble pageRankContribution = new NodeOrDouble(value.pageRank/value.outgoingSize());
    		
    		// emit 
    		context.write(outgoingID, pageRankContribution);
    	}
    
    }
}
