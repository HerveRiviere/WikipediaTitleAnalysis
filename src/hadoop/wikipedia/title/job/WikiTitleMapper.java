/**
 * Wikipedia title words hit hadoop job 
 * Author : Herv?? RIVIERE
 * Date : 30/03/14
 * Hadoop mapper class
 * 
 * Hadoop API used : mapreduce
 * one mapper per couple of <doc></doc>
 * Single reducer
 *  
 */

package hadoop.wikipedia.title.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WikiTitleMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value,org.apache.hadoop.mapreduce.Mapper.Context context)throws IOException, InterruptedException {

		List<String> listAnchor = parseXml(value);
		
		for (int i = 0; i < listAnchor.size(); i++) {
				word.set(listAnchor.get(i));
				context.write(word, one);
			}
		word.set("_____Number of articles______");
		context.write(word, one);
		word.set("_____Number of titles______");
		context.write(word, new IntWritable(listAnchor.size()));
		

	}

	
	public List<String> parseXml(Text value) {

		List<String> listAnchor = new ArrayList<String>();
		String word=null;
		String xmlString = value.toString();
		String[] listTitle = xmlString.split("<anchor>");
		for (int i = 1; i < listTitle.length; i++) {
			int stop = listTitle[i].indexOf('<');
			word=listTitle[i].substring(0, stop).toLowerCase();
			word=word.replace("&quot;", "");
			word=word.replace("&amp;", "");
			int isPortail=word.indexOf("portail:");
			if(word.length()>8 &&word.indexOf("/article")>-1 &&isPortail>-1){
			word=word.substring(isPortail+8,word.indexOf("/article"));}
			int isWikipedia=word.indexOf("wikip??dia:");
			if(isWikipedia>-1)word=word.substring(isWikipedia+10);
			listAnchor.add(word);

		}
		return listAnchor;

	}
	
	
	

}
