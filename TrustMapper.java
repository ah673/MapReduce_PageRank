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
    	
    	context.getCounter(HadoopCounter.COUNTERS.NUM_OF_NODES).increment(1);
    	// sink node
    	if (value.outgoing.length == 0){
    		double sinkNodePR = value.pageRank; 
    		// make PR a long to store in counter
    		long bloatedPR = (long) (sinkNodePR * 100000);
    		context.getCounter(HadoopCounter.COUNTERS.LEFTOVER_PAGE_RANK).increment(bloatedPR);
    	}
    	
    	// First output : Pass along (nodeID, Node)
    	context.write(key, new NodeOrDouble(value));
    	
    	// Second output : Pass along Pagerank
    	int[] outgoingLinks = value.outgoing; 
    	
    	for (int nodeID : outgoingLinks){
    		// construct output
    		IntWritable outgoingID = new IntWritable(nodeID);
    		double pageRankMass = value.pageRank/value.outgoingSize();
    		NodeOrDouble pageRankContribution = new NodeOrDouble(pageRankMass);
    		
    		// emit 
    		context.write(outgoingID, pageRankContribution);
    	}
    
    }
}
