/**
 * Global counters for MapReduce job. 
 * @author Amy Ho
 *
 */
public class HadoopCounter {

	public static enum COUNTERS {
		NUM_OF_NODES, // Keeps track of number of nodes in Graph. 
		LEFTOVER_PAGE_RANK // PageRank of sink nodes that must be redistributed
	}

}
