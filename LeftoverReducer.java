import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Implementation of calculation of PageRank
 * Equation 5.1 of the chapter on PageRank for reference
 * @author Amy Ho
 *
 */
public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    // This is the alpha that controls the "random jump" or our web surfer.
    public static double alpha = 0.85;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
    	// get number of nodes from Job Configuration
    	long totalNumOfNodes = Long.parseLong(context.getConfiguration().get("size"));
    	
    	//page rank
    	long bloatedLeftoverPR = Long.parseLong(context.getConfiguration().get("leftover"));
    	
    	//normalized leftover page rank
    	double leftoverPR = (bloatedLeftoverPR/100000.0); 
    	
    	// (m /|G|)
    	double PRtoDistribute = (leftoverPR)/totalNumOfNodes;
    	
    	// alpha * (1/|G|)
    	double randomJumpScore = alpha * (1.0/totalNumOfNodes); 
    	
    	double oneMinusAlpha = (1-alpha);  
    	
    	for (Node node : Ns){
    		double updatedPR = randomJumpScore + oneMinusAlpha*(PRtoDistribute + node.pageRank);
    		node.pageRank = updatedPR; 
    		context.write(nid, node);
    	}	
    	
    }
}
