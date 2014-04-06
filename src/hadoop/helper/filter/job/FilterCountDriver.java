/**
 * Hadoop mapper only filter job
 * 3 parameters : input file, minimum as integer, value as String (optional)
 * 
 * Input : flat file like  : KEY\tCOUNT 
 * Count as Integer, tab separator 
 * 
 * Output : 
 * VALUE\tKEY\tCOUNT 
 * such as COUNT > minimum (minimum values declared as parameter)
 * VALUE = third parameter
 * NB : all \t value in the key will be replaced by a space
 * 
 * 
 * Author : Hervé RIVIERE
 * Date : 30/03/14
 * Hadoop job main class, launch the mapper 
 * 
 * Hadoop API used : mapreduce
 *  
 */
package hadoop.helper.filter.job;

import java.util.Date;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class FilterCountDriver {

	public static void run(String[] args) throws Exception {

		
		 if (args.length < 2) {
		 System.err.println("File.jar fileLocation fileOutput");
		 ToolRunner.printGenericCommandUsage(System.err);
		 
		 } else {
		
		Date date = new Date();
		String valueToAdd="";
		//Job cofiguration
		Configuration conf = new Configuration();
		conf.set("mapred.input.dir", args[0]);
		conf.set("mapred.output.dir", args[0] + "_filtered_"+args[1]+"_"+date.getTime());
		conf.set("filter.minValue",args[1]);
		if(args.length>2)valueToAdd=args[2]+"\t";
		conf.set("filter.valueToAdd",valueToAdd);
		
		//Job creation
		Job job = Job.getInstance(conf);

		// Set jar main class to locate it in the distributed environment
		job.setJarByClass(FilterCountDriver.class);
		// Set job name to locate it in the distributed environment
		job.setJobName("WikiTitle");

		// Set input and output format, note that we use the overrided XmlInputFormat 
		//each record: text between xmlinput.start and xmlinput.end (whatever the xml structure !)
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(0);

		job.setMapperClass(FilterCountMapper.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }
	}

	public static void main(String[] args) throws Exception {

		/*String[] argSample = new String[3];
		argSample[0] = "testFiles/testFile_FilterCount";
		argSample[1] = "10";
		argSample[2] = "French";*/
		run(args);

	}

}
