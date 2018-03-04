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
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.hadoop.io.LongWritable;


public class Step5 {

	private final static float S = (float) 2/3;
	public static void main(String[] args) throws Exception {

		FileInputStream fstream = new FileInputStream(args[0]); /// make this map-red
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		HashMap<String,HashMap<String, List<String>>> hooksAndClusters = new HashMap<String, HashMap<String,List<String>>>();
		String strLine;
		String[] splittedLine;
		String[] splittedPatternsTargets;
		String[] currPatternAndTarget;
		String hookWord, patternsAndTargets;
		String target, pattern;
		Gson gson = new Gson();

		
		///first - read by-paterns and group togheter
		while ((strLine = br.readLine()) != null) {   
			if (strLine.length() < 2)
				continue;
			splittedLine = strLine.split("\\t");
			hookWord = splittedLine[0];
			hooksAndClusters.put(hookWord, new HashMap<String,List<String>>());
			HashMap<String, List<String>> currHookHashMap = hooksAndClusters.get(hookWord);// cluster is {target,[array of patterns]}    
//for each hook , cluster patternt by target
			patternsAndTargets = splittedLine[1];
			splittedPatternsTargets = patternsAndTargets.split("\\|");
			for (String pair : splittedPatternsTargets) {
				currPatternAndTarget = pair.split("##");
				pattern = currPatternAndTarget[0];
				target = currPatternAndTarget[1];
				if (currHookHashMap.containsKey(target)) {
					if (!currHookHashMap.get(target).contains(pattern))
							currHookHashMap.get(target).add(pattern);
				}
				else {
					currHookHashMap.put(target, new ArrayList<String>());  // new cluster
					currHookHashMap.get(target).add(pattern);
				}
			}

		}
				BufferedWriter out = new BufferedWriter(new FileWriter("output/step5/outbefore.txt"));
				out.write(gson.toJson(hooksAndClusters));
				out.close();
				
	//merge per hook worde 2/3 common		 ? check for fix point	
		// Second
		//hook                //targets   //patterns
		for(Entry<String, HashMap<String, List<String>>> hookANDtargetPattens : hooksAndClusters.entrySet()) {
			Iterator<Entry<String, List<String>>> innerIterator = hookANDtargetPattens.getValue().entrySet().iterator();
			//now i have iterator for all target and patterns for specific hook 
			while (innerIterator.hasNext()) {
				Map.Entry<String, List<String>> curr = innerIterator.next(); //target and list of patterns
				while (innerIterator.hasNext()) {
					Map.Entry<String, List<String>> next = innerIterator.next();
					if (shouldmerge(curr.getValue(), next.getValue())) { // if true merge and delete smaller
						curr.getValue().removeAll(next.getValue());
						curr.getValue().addAll(next.getValue());
						hookANDtargetPattens.getValue().remove(next.getKey());
						innerIterator = hookANDtargetPattens.getValue().entrySet().iterator();
						break;
					}      ///changing something while iterate over it , maybe not good
				}
			}  

		}

		BufferedWriter out2 = new BufferedWriter(new FileWriter("output/step5/outafter.txt"));
		Type type = new TypeToken<HashMap<String,HashMap<String, List<String>>>>(){}.getType();
		out2.write(gson.toJson(hooksAndClusters,type));
		out2.close();
	}

	private static boolean shouldmerge(List<String> curr, List<String> next) {
		List<String> big;
		List<String> small;

		float denominator;
		float numerator;
		//if small\big such that 2\3 share same patterns - merge
		if( curr.size() < next.size()) {
			big = next;
			small  = curr;
			numerator = 0;		
		}
		else {                        //   2 = comons    2
			big = curr;             //    3 not comon  5-2
			small  = next;
			numerator = 0;
		}

		for(String pattern: small)
			if(big.contains(pattern))
				numerator++;

		denominator = big.size() + small.size() - numerator;

		if (numerator/denominator <= S) 
			return false;

		return true; 


	}
}
