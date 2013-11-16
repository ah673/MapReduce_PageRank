import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustReducer extends Reducer<IntWritable, NodeOrDouble, IntWritable, Node> {
    public void reduce(IntWritable key, Iterable<NodeOrDouble> values, Context context)	throws IOException, InterruptedException {
	//implement your version of the reducer here.
    	System.out.println("==========Trust Reducer ==========");
    	
    	Node m = null; 
    	double sum = 0; 
    	
    	for (NodeOrDouble nodeDouble : values){
    		if (nodeDouble.isNode()){
    			// node
    			m = nodeDouble.getNode();
    			
    		} else {
    			// sum incoming page rank contributions
    			sum += nodeDouble.getDouble(); 
    		}
    	}
    	
		m.pageRank = sum;
    	
    	// output 
    	context.write(new IntWritable(m.nodeid), m); 
    	
    }
}
