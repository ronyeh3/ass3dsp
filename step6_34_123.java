import java.awt.Toolkit;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


//remove pattern that appers in only 1 curpuse


public class step6_34_123 {
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

		/////////////////////////////////////////////////


		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith("byPattern");}};

				File folder = new File(args[1]);
				File[] listOfFiles = folder.listFiles(filter);
				for (int i = 0; i < listOfFiles.length; i++) {
					File file = listOfFiles[i];
					FileInputStream fstream2 = new FileInputStream(file);
					BufferedReader br2 = new BufferedReader(new InputStreamReader(fstream2));

					while ((strLine = br2.readLine()) != null) {    //Remove all patterns originating from a single hook corpus only
						if (strLine.length() < 2)
							continue;
						splittedLine = strLine.split("\t");
						pattern = splittedLine[0];
						splittedtargetbyHook = splittedLine[1].split("\\|");
						if (splittedtargetbyHook.length > 1)
							continue;
						String singlehookword = splittedtargetbyHook[0];
						Iterator<Entry<String, Object>> innerIterator;
						//cluster is a target and a set of patterns, for each hook we have several clusters
						if(hooksAndClusters.containsKey(singlehookword))
						innerIterator = hooksAndClusters.get(singlehookword).entrySet().iterator();
						//now i have iterator for target and patterns for specific hook 
						else
							continue;
						while (innerIterator.hasNext()) {
							((List<String>)innerIterator.next().getValue()).remove(pattern);
						}
					}
					br2.close();
				}


				///mark all patterns as unconfirmed
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



				//////Type type2 = new TypeToken<HashMap<String,HashMap<String, Pair<String,Integer>>>>(){}.getType(); //no need
				//					BufferedWriter out2 = new BufferedWriter(new FileWriter("output/step6_34_123/outbefore342.txt"));
				//					out2.write(gson.toJson(hooksAndClusters));
				//					out2.close();

				///////////////////////////////////////////////////////////////////////////				
				System.out.println("start 3.4 big loop");
				//3.4.3
				//target   //pattern //unconfirmed/core
				System.out.println("Searching for minimal unconfirmed");
				Pair<String,Entry<String,List<Pair<String,Integer>>>> minimalUnconfirmedCluster;

				while ((minimalUnconfirmedCluster = getMinimalUnconfirmedCluster(hooksAndClusters)) != null) {
					//System.out.println("Got a minimal unconfirmed");
					for(Entry<String, HashMap<String, Object>> cuurHookANDtargetPatterns : hooksAndClusters.entrySet()) {
						if (cuurHookANDtargetPatterns.getKey().equals(minimalUnconfirmedCluster.getFirst())) //minimalUnconfirmedCluster.first is the hook that the cluster extracted from
							continue; //(from different hook)

						Iterator<Entry<String, Object>> innerIterator = cuurHookANDtargetPatterns.getValue().entrySet().iterator();
						while (innerIterator.hasNext()) {
							Map.Entry<String, Object> otherCluster = innerIterator.next(); //target and list of patterns
							if(shouldmerge(minimalUnconfirmedCluster.getSecond().getValue(), (List<Pair<String,Integer>>)otherCluster.getValue())) {
								//	System.out.println("Decided to merge!!");
								//	System.out.println("hookword1: "+minimalUnconfirmedCluster.getFirst() + "  hookword2: "+cuurHookANDtargetPatterns.getKey());
								//	System.out.println("targetword1: "+minimalUnconfirmedCluster.getSecond().getKey()+" t argetword2: "+otherCluster.getKey());

								mergeListPatternClusters(minimalUnconfirmedCluster.getSecond().getValue(), (List<Pair<String,Integer>>)otherCluster.getValue());
								cuurHookANDtargetPatterns.getValue().remove(otherCluster.getKey());
								innerIterator = cuurHookANDtargetPatterns.getValue().entrySet().iterator();
								//break; //???? if merge starit from beginig??????????????
							}

						}
					}
					if (isStillUnconfirmed(minimalUnconfirmedCluster)) { // step 4 in the algorithm : is c1 still unconfirmed? if yes, delete it
						String hookWord = minimalUnconfirmedCluster.getFirst();
						String targetWord = minimalUnconfirmedCluster.getSecond().getKey(); // the name of the cluster to delete
						hooksAndClusters.get(hookWord).remove(targetWord);
						//System.out.println("deleting : "+ targetWord);
					}
				}


				printoutputcluster(hooksAndClusters , gson);

				//beep sounds when finish
				for(int i=0; i<50;i++) {
					Toolkit.getDefaultToolkit().beep();
					Thread.sleep(1000);
				}
	}



	//print list of clusters, for all cluster there are 2 list , such that first place is list of all confirmed , second list is the unconfirmd
	private static void printoutputcluster(HashMap<String, HashMap<String, Object>> hooksAndClusters, Gson gson) throws IOException {
		BufferedWriter out22 = new BufferedWriter(new FileWriter("output/step6_34_123/finish.txt"));

		List<List<String>> cluster = new ArrayList<List<String>>();
		List<String> confiremd = new ArrayList<String>();
		List<String> unconfiremd = new ArrayList<String>();
		cluster.add(confiremd); cluster.add(unconfiremd);

		Pair<String, Integer> currPattern;
		for(Entry<String, HashMap<String, Object>> currhookANDtargetPatterns : hooksAndClusters.entrySet()) { //hook
			Iterator<Entry<String, Object>> innerIterator = currhookANDtargetPatterns.getValue().entrySet().iterator();
			while (innerIterator.hasNext()) { //cluster
				Map.Entry<String, Object> currCluster = innerIterator.next();
				List<Pair<String,Integer>> currPatternList = (List<Pair<String,Integer>>) currCluster.getValue();
				for (int i=0 ; i< currPatternList.size(); i++) {
					currPattern = currPatternList.get(i);
					if (currPattern.getSecond() == 1) {
						confiremd.add(currPattern.getFirst());	
					}
					else
						unconfiremd.add(currPattern.getFirst());

				}//finish one cluter
				out22.write(gson.toJson(cluster));
				confiremd.clear();
				unconfiremd.clear();

			}//iterate to next cluster
		}

		out22.close();

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

		List<Pair<String, Integer>> confiremd = new ArrayList<Pair<String, Integer>>();


		String c1Patt, c2Patt;
		Pair<String,Integer> c1Pattern, c2Pattern;
		for (int i=0 ; i<c1.size(); i++) {
			c1Pattern = c1.get(i);
			c1Patt = (String) c1Pattern.getFirst();
			for (int j=0 ; j<c2.size() ; j++) {
				c2Pattern = c2.get(j);
				c2Patt = (String) c2Pattern.getFirst();
				if(c1Patt.equals(c2Patt)) {
					c1Pattern.setSecond(1);
					c2.remove(c2Pattern);
					c2.remove(c1Pattern);
					//change anything else to unconfiermed
				}
			}
		}

		//add to list c1 all items from c2 that are not confiremed
		c2.forEach((a)->a.setSecond(0));
		c1.addAll(c2);
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
		float ans =  numerator/denominator;
		//System.out.println(ans);
		if(ans < S) 
			return false;

		return true; 
	}

	private static Pair<String, Entry<String, List<Pair<String, Integer>>>> getMinimalUnconfirmedCluster(HashMap<String, HashMap<String, Object>> hooksAndClusters) {
		int minimal = Integer.MAX_VALUE;
		boolean allPatternsUnconfirmed;
		Pair<String,Integer> currPattern;
		Pair<String, Entry<String, List<Pair<String, Integer>>>> ans = null;
		//hoook        //target       //patternim with status
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
					}
					if (allPatternsUnconfirmed) {
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