import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustMapper extends Mapper
<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map
    (IntWritable key, Node value, Context context) throws IOException, InterruptedException {

        // Implement
        
        // Part 1/2: emit the node itself
        
        // Emit this node for reconstructing node structure
        // node needs to be transformed to NodeorDouble class
        // so that the TrustReducer can receiv either a node or double
        context.write(key, new NodeOrDouble(value));
        
        // also increase the numNodes counter by 1
        context.getCounter(HadoopCounter.COUNTERS.numNodes).increment(1);
        
        // Part 2/2: emit the node's pageRank
        
        // See if the node is a dangling node and 
        // do things accordingly
        int outgoingSize = value.outgoingSize();
        
        if(outgoingSize == 0){ // this is a dangling node
			
			// Use a Hadoop counter to retain the "lost"/unemitable pageRank
			// from this dangling node 
			
			// Get the node's pageRank and transform it to a long format
			// because Hadoop counter only deal with 8-byte integers
			double nodePR = value.getPageRank();
			System.out.println("this dangling node has PR of " + nodePR);
			long nodePRLong = (long) (nodePR * 1000000); // X 1 million
			
			// Send the nodePGLong to counter
			context.getCounter(HadoopCounter.COUNTERS.leftoverPR).increment(nodePRLong);
			System.out.println("leftoverPR counter should increase by " + nodePRLong);
			
			
        } else { // this is a normal node
			
			// its pageRank emission for each outgoing edge
			double pR = value.getPageRank()/outgoingSize;
			
			// emit pageRank to each neighbour
			for(int neighbour: value.outgoing){
				// Need to transform neighbour to IntWriteable
				// and pR to NodeOrDouble
				context.write(new IntWritable(neighbour), new NodeOrDouble(pR));
			}
		}
    }
}
