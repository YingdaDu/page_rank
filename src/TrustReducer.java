import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustReducer extends Reducer
<IntWritable, NodeOrDouble, IntWritable, Node> {
    public void reduce
    (IntWritable key, Iterable<NodeOrDouble> values, Context context)
        throws IOException, InterruptedException {
        
        //Implement
        
        // Initiate an empty node and zero pageRank value
        // they will be replaced by a real node and incremented to a new value
        // when the reducer iterating through key-value pairs it receives
        Node n = null;
        double pR = 0;
        
        // Iterate over all key-value pairs sent to this node
        for(NodeOrDouble nD : values){
			if(nD.isNode()){ // nd is a node
				n = nD.getNode();
			} else{ // nd is a pageRank value
				pR += nD.getDouble();
			}
		
		}
		
		// After iterating finishes, setup node's new pageRank and emit the node
        n.setPageRank(pR);
        System.out.println("this node is No. " + n.nodeid);
        System.out.println("after even pass, it has pR of " + pR);
        context.write(new IntWritable(n.nodeid), n);
    }
}
