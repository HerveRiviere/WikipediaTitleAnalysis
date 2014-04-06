/**
 * Wikipedia title words hit hadoop job 
 * Author : Herv?? RIVIERE
 * Date : 30/03/14
 * Hadoop reducer class
 * 
 * Hadoop API used : mapreduce
 * one mapper per couple of <doc></doc>
 * Single reducer
 *  
 */


package hadoop.wikipedia.title.job;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikiTitleReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
		   int sum = 0;
	        Iterator<IntWritable> itr = values.iterator();
	        while (itr.hasNext()) {
	            sum += itr.next().get();
	        }
	        context.write(key, new IntWritable(sum));
    }
	
}
