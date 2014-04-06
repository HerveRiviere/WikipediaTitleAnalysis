package hadoop.wikipedia.title.analysis.core.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import hadoop.wikipedia.title.job.WikiTitleReducer;

import org.apache.hadoop.io.*;
import org.junit.Test;

public class TestWikiTitleReducer {

	final ArrayList<String> test = new ArrayList<String>();
	
	@Test
	public void testReducer() throws IOException, InterruptedException{

		WikiTitleReducer reducer=new WikiTitleReducer();
		WikiTitleReducer.Context context = Mockito.mock(WikiTitleReducer.Context.class);

		Mockito.doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				Object[] args = invocation.getArguments();
				test.add(args[0].toString()+" "+args[1].toString());
				return "called with arguments: " + args;
			}
		}).when(context).write(Mockito.any(Text.class), Mockito.any(IntWritable.class));

		List<IntWritable> values = Arrays.asList(new IntWritable(1),new IntWritable(2));
		
		reducer.reduce(new Text("test"), values, context);
		

		ArrayList<String> actualMap = new ArrayList<String>();
		
		actualMap.add("test 3");
		assertEquals(actualMap, test);

	}

}
