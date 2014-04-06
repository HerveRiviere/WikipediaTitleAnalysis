package hadoop.wikipedia.title.analysis.core.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import hadoop.wikipedia.title.job.WikiTitleMapper;

import org.apache.hadoop.io.*;
import org.junit.Test;

public class TestWikiTitleMapper {

	final ArrayList<String> test = new ArrayList<String>();
	Text value = new Text("<doc><title>Wikipédia : The Corner (film, 1916)</title><url>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)</url><abstract>The Corner (titre original : Ugolok) est un film russe muet réalisé par Cheslav Sabinsky, sorti en 1916.</abstract><links><sublink linktype=\"nav\"><anchor>Synopsis</anchor><link>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)#Synopsis</link></sublink><sublink linktype=\"nav\"><anchor>Fiche_technique technique</anchor><link>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)#Fiche_technique</link></sublink><sublink linktype=\"nav\"><anchor>&quot;Distribution</anchor><link>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)#Distribution</link></sublink><sublink linktype=\"nav\"><anchor>Voir aussi</anchor><link>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)#Voir_aussi</link></sublink><sublink linktype=\"nav\"><anchor>Bibliographie</anchor><link>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)#Bibliographie</link></sublink><sublink linktype=\"nav\"><anchor>Liens externes</anchor><link>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)#Liens_externes</link></sublink><sublink linktype=\"nav\"><anchor>Notes et références</anchor><link>http://fr.wikipedia.org/wiki/The_Corner_(film,_1916)#Notes_et_r.C3.A9f.C3.A9rences</link></sublink></links><anchor>portail:Comédies musicales/articles liés</anchor><anchor>wikipédia:ébauche australie</anchor></doc>");
	@Test
	public void testMapper() throws IOException, InterruptedException {

		WikiTitleMapper mapper = new WikiTitleMapper();
		WikiTitleMapper.Context context = Mockito.mock(WikiTitleMapper.Context.class);

		Mockito.doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				Object[] args = invocation.getArguments();
				test.add(args[0].toString()+" "+args[1].toString());
				return "called with arguments: " + args;
			}
		}).when(context).write(Mockito.any(Text.class), Mockito.any(IntWritable.class));

		mapper.map(null, value, context);

		
		assertEquals(test.size(),11);
		assertEquals(test.get(0),"synopsis 1");
		assertEquals(test.get(1),"fiche_technique technique 1");
		assertEquals(test.get(2),"distribution 1");
		assertEquals(test.get(3),"voir aussi 1");
		assertEquals(test.get(4),"bibliographie 1");
		assertEquals(test.get(5),"liens externes 1");
		assertEquals(test.get(6),"notes et références 1");
		assertEquals(test.get(7),"comédies musicales 1");
		assertEquals(test.get(8),"ébauche australie 1");
		assertEquals(test.get(9),"_____Number of articles______ 1");
		assertEquals(test.get(10),"_____Number of titles______ 9");
	}
	
	
	@Test
	public void testXMLParser(){
		
		WikiTitleMapper mapper = new WikiTitleMapper();
		List<String> returnList = mapper.parseXml(value);
		assertEquals(returnList.size(),9);
		assertEquals(returnList.get(0),"synopsis");
		assertEquals(returnList.get(1),"fiche_technique technique");
		assertEquals(returnList.get(2),"distribution");
		assertEquals(returnList.get(3),"voir aussi");
		assertEquals(returnList.get(4),"bibliographie");
		assertEquals(returnList.get(5),"liens externes");
		assertEquals(returnList.get(6),"notes et références");
		assertEquals(returnList.get(7),"comédies musicales");
		assertEquals(returnList.get(8),"ébauche australie");
	}

}
