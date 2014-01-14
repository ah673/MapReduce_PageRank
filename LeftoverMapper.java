import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper - performs identity mapping. 
 * @author Amy Ho
 *
 */
public class LeftoverMapper extends Mapper<IntWritable, Node, IntWritable, Node> {

    public void map(IntWritable nid, Node N, Context context) throws IOException, InterruptedException {
    	// output to preserve graph structure
    	context.write(nid, N);
    }
}
