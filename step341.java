import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.hadoop.io.LongWritable;

//remove pattern that appers in only 1 curpuse
public class step341 {

	public static void main(String[] args) throws Exception {
		Gson gson = new Gson();
		FileInputStream fstream = new FileInputStream(args[0]);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		HashMap<String,HashMap<String, List<String>>> hooksAndClusters = new HashMap<String, HashMap<String,List<String>>>();
		String strLine;

		String[] splittedLine;
		String[] splittedtargetbyHook;

		String  pattern;
		Type type = new TypeToken<HashMap<String,HashMap<String, List<String>>>>(){}.getType();
		hooksAndClusters = gson.fromJson(br, type);

		FileInputStream fstream2 = new FileInputStream(args[1]);
		BufferedReader br2 = new BufferedReader(new InputStreamReader(fstream2));

		while ((strLine = br2.readLine()) != null) {
			if (strLine.length() < 2)
				continue;
			splittedLine = strLine.split("\\t");
			pattern = splittedLine[0];
			splittedtargetbyHook = splittedLine[1].split("\\|");
			if (splittedtargetbyHook.length > 1)
				continue;
			String singlehookword = splittedtargetbyHook[0];



			Iterator<Entry<String, List<String>>> innerIterator = hooksAndClusters.get(singlehookword).entrySet().iterator();
			//now i have iterator for target and patterns for specific hook 
			while (innerIterator.hasNext()) {
				innerIterator.next().getValue().remove(pattern);
			}
		}

		BufferedWriter out2 = new BufferedWriter(new FileWriter("output/step341/outbefor.txt"));
		out2.write(gson.toJson(hooksAndClusters,type));
		out2.close();


	}

}






//			for (String pair : splittedPatternsTargets) {
//				currPatternAndTarget = pair.split("##");
//				pattern = currPatternAndTarget[0];
//				target = currPatternAndTarget[1];
//				if (currHookHashMap.containsKey(target)) {
//					currHookHashMap.get(target).add(pattern);
//				}
//				else {
//					currHookHashMap.put(target, new ArrayList<String>());
//					currHookHashMap.get(target).add(pattern);
