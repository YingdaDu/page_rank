import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    public static double alpha = 0.85;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
        //Implement
        
        // Part 1: calculate pageRank from dangling nodes and pageRanks from
        // random jump
        
        long numNodes = Long.parseLong(context.getConfiguration().get("size"));
        System.out.println("number of nodes is " + numNodes);
        long leftoverPRLong = Long.parseLong(context.getConfiguration().get("leftover"));
		double leftoverPR = leftoverPRLong/1000000.0; // long to double
		System.out.println("leftoverPR to be distributed is " + leftoverPR);
		
		double randomJumpPRPerNode = 1.0/numNodes;
		double leftoverPRPerNode = leftoverPR/numNodes;
		
		// Part 2: update every nodes' pageRank and emit the node
		for(Node n: Ns){
			double newPR;
			newPR = alpha*randomJumpPRPerNode + (1-alpha)*(leftoverPRPerNode + n.getPageRank());
			n.setPageRank(newPR);
			context.write(nid, n);
		}
    }
}
