import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

import org.apache.hadoop.io.LongWritable;


public class Step5 {


	public static void main(String[] args) throws Exception {

		FileInputStream fstream = new FileInputStream(args[0]);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		HashMap<String,HashMap<String, List<String>>> hooksAndClusters = new HashMap<String, HashMap<String,List<String>>>();
		String strLine;
		String[] splittedLine;
		String[] splittedPatternsTargets;
		String[] currPatternAndTarget;
		String hookWord, patternsAndTargets;
		String target, pattern;
		Gson gson = new Gson();

		while ((strLine = br.readLine()) != null) {
			if (strLine.length() < 2)
				continue;
			splittedLine = strLine.split("\\t");
			hookWord = splittedLine[0];
			hooksAndClusters.put(hookWord, new HashMap<String,List<String>>());
			HashMap<String, List<String>> currHookHashMap = hooksAndClusters.get(hookWord);
			System.out.println(hookWord);
			patternsAndTargets = splittedLine[1];
			splittedPatternsTargets = patternsAndTargets.split("\\|");
			for (String pair : splittedPatternsTargets) {
				currPatternAndTarget = pair.split("##");
				pattern = currPatternAndTarget[0];
				target = currPatternAndTarget[1];
				if (currHookHashMap.containsKey(target)) {
					currHookHashMap.get(target).add(pattern);
				}
				else {
					currHookHashMap.put(target, new ArrayList<String>());
					currHookHashMap.get(target).add(pattern);
				}
			}

		}
		//BufferedWriter out = new BufferedWriter(new FileWriter("output/step5/out.txt"));
		//out.write(gson.toJson(hooksAndClusters));
		
		// Second
		
		Iterator it = hooksAndClusters.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String, HashMap<String, List<String>>> currentHookAndClusters = (Entry<String, HashMap<String, List<String>>>)it.next();
	        Iterator innerIterator = currentHookAndClusters.getValue().entrySet().iterator();
	        while (innerIterator.hasNext()) {
	        	next
	        	
	        	shouldmerge(curr, next)
	        	
		        <Target, List<Patterns>>
	        	
	        	Iterator innerIterator = currentHookAndClusters.getValue().entrySet().iterator();
	        	if (shouldMerge(i))
	        }
	        
	        
	    }

	}
}
