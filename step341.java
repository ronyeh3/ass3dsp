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

import com.amazonaws.services.opsworks.model.App;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.hadoop.io.LongWritable;


//remove pattern that appers in only 1 curpuse
public class step341 {
	private static final float S = (float) 2/3;

	public static void main(String[] args) throws Exception {
		Gson gson = new Gson();
		FileInputStream fstream = new FileInputStream(args[0]);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		HashMap<String,HashMap<String, Object>> hooksAndClusters = new HashMap<String, HashMap<String,Object>>();
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



			Iterator<Entry<String, Object>> innerIterator = hooksAndClusters.get(singlehookword).entrySet().iterator();
			//now i have iterator for target and patterns for specific hook 
			while (innerIterator.hasNext()) {
				((List<String>)innerIterator.next().getValue()).remove(pattern);
			}
		}




		for(Entry<String, HashMap<String, Object>> hookANDtargetPatterns : hooksAndClusters.entrySet()) {
			Iterator<Entry<String, Object>> innerIterator = hookANDtargetPatterns.getValue().entrySet().iterator();
			while (innerIterator.hasNext()) {
				Map.Entry<String, Object> curr = innerIterator.next();
				List<String> currentListOfPatterns = (List) curr.getValue();
				List<Pair<String,Integer>> newListOfPatterns = new ArrayList<Pair<String,Integer>>();
				for (String currPattern : currentListOfPatterns) {
					newListOfPatterns.add(new Pair<String, Integer>(currPattern, 0));
				}
				curr.setValue(newListOfPatterns);

			}


		}
		//Type type2 = new TypeToken<HashMap<String,HashMap<String, Pair<String,Integer>>>>(){}.getType();



		//		BufferedWriter out2 = new BufferedWriter(new FileWriter("output/step342/outbefor.txt"));
		//		out2.write(gson.toJson(hooksAndClusters));
		//		out2.close();
		
		
		//target   //pattern //unconfirmed/core
		System.out.println("Searching for minimal unconfirmed");
		Pair<String,Entry<String,List<Pair<String,Integer>>>> minimalUnconfirmedCluster;
		while ((minimalUnconfirmedCluster = getMinimalUnconfirmedCluster(hooksAndClusters)) != null) {
			System.out.println("Got a minimal unconfirmed");
			for(Entry<String, HashMap<String, Object>> hookANDtargetPatterns : hooksAndClusters.entrySet()) {
				if (hookANDtargetPatterns.getKey().equals(minimalUnconfirmedCluster.getFirst()))
					continue;
				Iterator<Entry<String, Object>> innerIterator = hookANDtargetPatterns.getValue().entrySet().iterator();
				while (innerIterator.hasNext()) {
					Map.Entry<String, Object> curr = innerIterator.next();
					if(shouldmerge(minimalUnconfirmedCluster.getSecond().getValue(), (List<Pair<String,Integer>>)curr.getValue())) {
						System.out.println("Decided to merge!!");
						mergeListPatternClusters(minimalUnconfirmedCluster.getSecond().getValue(), (List<Pair<String,Integer>>)curr.getValue());
						hookANDtargetPatterns.getValue().remove(curr.getKey());
						innerIterator = hookANDtargetPatterns.getValue().entrySet().iterator();
					}

				}
			}
			if (isStillUnconfirmed(minimalUnconfirmedCluster)) { // step 4 in the algorithm : is c1 still unconfirmed? if yes, delete it
				String hookWord = minimalUnconfirmedCluster.getFirst();
				String targetWord = minimalUnconfirmedCluster.getSecond().getKey(); // the name of the cluster to delete
				hooksAndClusters.get(hookWord).remove(targetWord);
			}
		}


		BufferedWriter out2 = new BufferedWriter(new FileWriter("output/step34c/outbefor.txt"));
		out2.write(gson.toJson(hooksAndClusters,type));
		out2.close();



	}

	private static boolean isStillUnconfirmed( Pair<String, Entry<String, List<Pair<String, Integer>>>> unconfirmedCluster) {
		List<Pair<String,Integer>> patterns = unconfirmedCluster.getSecond().getValue();
		for (Pair<String,Integer> pattern : patterns) {
			if (pattern.getSecond() == 1) {
				return false;
			}
		}
		return true;

	}

	private static void mergeListPatternClusters(List<Pair<String, Integer>> c1, List<Pair<String, Integer>> c2) {
		String c1Patt, c2Patt;
		Pair<String,Integer> c1Pattern, c2Pattern;
		for (int i=0 ; i<c2.size(); i++) {
			c2Pattern = c2.get(i);
			c2Patt = (String) c2Pattern.getFirst();
			for (int j=0 ; j<c1.size() ; j++) {
				c1Pattern = c1.get(j);
				c1Patt = (String) c1Pattern.getFirst();
				if(c1Patt.equals(c2Patt)) {
					c1Pattern.setSecond(1);
				}
				else {
					c2Pattern.setSecond(0);
					c1.add(c2Pattern);
				}
			}
		}

	}

	public static boolean shouldmerge(List<Pair<String, Integer>> c1, List<Pair<String, Integer>> c2) {
		String c1Patt, c2Patt;
		boolean allCoresAreShared = true;
		float denominator;
		float numerator = 0;	
		Pair<String,Integer> c2Pattern;
		Pair<String,Integer> c1Pattern;
		//if small\big such that 2\3 share same patterns - merge

		// we want to merge c1 and c2 if they have 2/3 of their patterns shared AND all c2 cores are shared.

		for (int i=0 ; i<c2.size() ; i++) {
			c2Pattern = c2.get(i);
			c2Patt = (String) c2Pattern.getFirst();
			if (c2Pattern.getSecond() == 1) {
				allCoresAreShared = false;
			}
			for (int j=0 ; j<c1.size() ; j++) {
				c1Pattern = c1.get(j);
				c1Patt = (String) c1Pattern.getFirst();
				if(c1Patt.equals(c2Patt)) {
					numerator++;
					allCoresAreShared = true;
				}
			}
			if (!allCoresAreShared) {
				return false;
			}
		}
		//TODO Check how we should calculate the percentage of shared. From the minimal list or the total num of elements.
		denominator = c1.size() + c2.size() - numerator;
		System.out.println((float) numerator/denominator);
		if((float) numerator/denominator < S) 
			return false;

		return true; 
	}

	private static Pair<String, Entry<String, List<Pair<String, Integer>>>> getMinimalUnconfirmedCluster(HashMap<String, HashMap<String, Object>> hooksAndClusters) {
		int minimal = Integer.MAX_VALUE;
		boolean allPatternsUnconfirmed;
		Pair<String,Integer> currPattern;
		Pair<String, Entry<String, List<Pair<String, Integer>>>> ans = null;

		for(Entry<String, HashMap<String, Object>> hookANDtargetPatterns : hooksAndClusters.entrySet()) {
			for(Entry<String, Object> targetANDPatterns : hookANDtargetPatterns.getValue().entrySet()) {
				List<Pair<String,Integer>> currPatternList = (List<Pair<String,Integer>>) targetANDPatterns.getValue();
				if (currPatternList.size() < minimal) { // if the currlist is bigger than minimal, it is not a candidate
					allPatternsUnconfirmed = true; // initializing
					for (int i=0 ; i< currPatternList.size() && allPatternsUnconfirmed; i++) {
						currPattern = currPatternList.get(i);
						if (currPattern.getSecond() == 1) {
							allPatternsUnconfirmed = false; // caught confirmed (core) pattern, not all patterns are unconfirmed, break.
							break;
						}
						// if we reached here we found a new minimal unconfirmed cluster
						minimal = currPatternList.size();
						ans = new Pair(hookANDtargetPatterns.getKey(), targetANDPatterns);
					}
				}
			}
		}
		// at this point, if we found a miminalUnconfirmedCluster it set in the variable ans, otherwise ans is null
		return ans;
	}
}