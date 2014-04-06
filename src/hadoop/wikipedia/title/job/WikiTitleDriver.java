/**
 * Wikipedia title words hit hadoop job 
 * Author : Herv?? RIVIERE
 * Date : 30/03/14
 * Hadoop job main class, launch the mapper and the reducer
 * 
 * Hadoop API used : mapreduce
 * one mapper per couple of <doc></doc>
 * Single reducer
 * 
 *  The mapper use a override XmlInputFormat class as input format (from mahout project), 
 *  source can be found here : http://svn.apache.org/repos/asf/mahout/trunk/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java
 *  
 */
package hadoop.wikipedia.title.job;

import hadoop.helper.xml.XmlInputFormat;

import java.util.Date;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class WikiTitleDriver {

	public static void run(String[] args) throws Exception {

		
		 if (args.length != 2) {
		 System.err.println("File.jar fileLocation fileOutput");
		 ToolRunner.printGenericCommandUsage(System.err);
		 
		 } else {
		
		Date date = new Date();
		
		//Job cofiguration
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<doc>");
		conf.set("xmlinput.end", "</doc>");
		conf.set("mapred.input.dir", args[0]);
		conf.set("mapred.output.dir", args[1] + date.getTime());
		
		//Job creation
		Job job = Job.getInstance(conf);

		// Set jar main class to locate it in the distributed environment
		job.setJarByClass(WikiTitleDriver.class);
		// Set job name to locate it in the distributed environment
		job.setJobName("WikiTitle");

		// Set input and output format, note that we use the overrided XmlInputFormat 
		//each record: text between xmlinput.start and xmlinput.end (whatever the xml structure !)
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		job.setMapperClass(WikiTitleMapper.class);
		job.setReducerClass(WikiTitleReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }
	}

	public static void main(String[] args) throws Exception {

		/*String[] argSample = new String[2];
		argSample[0] = "testFiles/testFile_frwiki-latest-abstract5.xml";
		argSample[1] = "out";*/
		run(args);

	}

}
