
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;


public class NodeInputFormat extends FileInputFormat<IntWritable, Node> {
    public RecordReader<IntWritable, Node> createRecordReader(InputSplit input, TaskAttemptContext ctx) throws IOException {
    	return new NodeRecordReader(); //Simply construct and return a NodeRecordReader
    }
}