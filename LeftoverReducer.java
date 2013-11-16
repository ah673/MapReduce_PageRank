import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    // This is the alpha that controls the "random jump" or our web surfer.
    // See equation 5.1 of the chapter on PageRank
    public static double alpha = 0.85;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
	//implement your version of the reduce here!
    	System.out.println("************ LEFTOVER REDUCING *************");
    	
    	long totalNumOfNodes = Long.parseLong(context.getConfiguration().get("size"));
    	System.out.println("totalNumOfNodes " + totalNumOfNodes); 
    	
    	
    	//page rank
    	long bloatedLeftoverPR = Long.parseLong(context.getConfiguration().get("leftover"));
    	System.out.println("bloatedLeftoverPR " + bloatedLeftoverPR);
    	//normalized leftover page rank
    	double leftoverPR = (bloatedLeftoverPR/100000.0); 
    	System.out.println("leftoverPR " + leftoverPR); 
    	double PRtoDistribute = (leftoverPR)/totalNumOfNodes;
    	System.out.println("PRtoDistribute " + PRtoDistribute); 
    	
    	// alpha * (1/|G|)
    	double randomJumpScore = alpha * (1.0/totalNumOfNodes); 
    	System.out.println("randomJumpScore " + randomJumpScore); 
    	
    	double oneMinusAlpha = (1-alpha);  
    	
    	for (Node node : Ns){
    		System.out.println(node); 
    		double updatedPR = randomJumpScore + oneMinusAlpha*(PRtoDistribute + node.pageRank);
    		System.out.println("updatedPR " + updatedPR);
    		node.pageRank = updatedPR; 
    		context.write(nid, node);
    	}	
    	
    }
}
