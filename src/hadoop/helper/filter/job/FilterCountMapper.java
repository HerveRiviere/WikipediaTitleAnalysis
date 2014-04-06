/**
 * Wikipedia title words hit hadoop job 
 * Author : Hervé RIVIERE
 * Date : 30/03/14
 * Hadoop mapper class
 * 
 * Hadoop API used : mapreduce
 * one mapper per couple of <doc></doc>
 * Single reducer
 *  
 */

package hadoop.helper.filter.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FilterCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private Integer filterMinValue=null;
	private String valueToAdd=null;
	
	@Override
	public void map(LongWritable key, Text value,org.apache.hadoop.mapreduce.Mapper.Context context)throws IOException, InterruptedException {
		getParameters(context);
		String[] word=value.toString().split("\t");

		int number=-1;
		if(word.length>2){
			number=Integer.parseInt(word[word.length-1]);
			for(int i=1; i<word.length-2;i++){
				word[0]+=" "+word[i];
			}
		}else{
		number = Integer.parseInt(word[1]);}
		if(number>filterMinValue){
			
		context.write(new Text(valueToAdd+word[0]),new IntWritable(number));}
		

	}
	
	
	private void getParameters(org.apache.hadoop.mapreduce.Mapper.Context context){
		Configuration conf = context.getConfiguration();
		filterMinValue=Integer.parseInt(conf.get("filter.minValue"));
		valueToAdd=conf.get("filter.valueToAdd");
		}

	
	
	
	
	

}
