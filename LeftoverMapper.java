import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class LeftoverMapper extends Mapper<IntWritable, Node, IntWritable, Node> {

    public void map(IntWritable nid, Node N, Context context) throws IOException, InterruptedException {
	//Implement your version of the map here
    	System.out.println("************ LEFTOVER MAPPER *************");
    	System.out.println("nid " + nid  + "\nNode " + N);
    	
    	// first output to preserve graph structure
    	context.write(nid, N);
    	
	return;
    }
}
