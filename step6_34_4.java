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
import java.util.HashSet;
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
import com.google.gson.JsonStreamParser;
import com.google.gson.reflect.TypeToken;

import org.apache.hadoop.io.LongWritable;


//last step (number 4) of 3.4 algoritm : If several clusters have the same set of core patterns
// merge them according to rules (i,ii).
//input : finish.txt. from step6_34_123
public class step6_34_4 {


	public static void main(String[] args) throws Exception {
		Gson gson = new Gson();
		FileInputStream fstream = new FileInputStream(args[0]);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		JsonStreamParser jp = new JsonStreamParser(br);
		ArrayList<List<List<String>>> allClusters =  new ArrayList<List<List<String>>>();  // list of clusters, each element is list of [ [list] , [list] ] 
		                                                                                     //yeah, list of 2 lists of lists                 conf     unconf
		Type type = new TypeToken<ArrayList<List<String>>>(){}.getType();
		while(jp.hasNext()) {
			allClusters.add(gson.fromJson(jp.next(), type));
		}

				System.out.println(allClusters);
				System.out.println("How many clustrs: "+allClusters.size());
				System.out.println(allClusters.get(0));
				System.out.println(allClusters.get(0).get(0));
				System.out.println(allClusters.get(0).get(0).get(0));

		statAgain:
			for( int i=0; i<allClusters.size();i++) {
				for( int j=0; j<allClusters.size();j++) {

					if (j==i) 
						continue; //diffrent then the minimal itself
					if(shouldmerge(allClusters.get(i), allClusters.get(j))) {
						System.out.println("Decided to merge!!");
						mergeListPatternClusters(allClusters.get(i), allClusters.get(j));
						allClusters.remove(allClusters.get(j));
						break statAgain;
					}

				}
			}

		//System.out.println(allClusters);
				BufferedWriter out2 = new BufferedWriter(new FileWriter("output/step344/last.txt"));
				out2.write(gson.toJson(allClusters));
				out2.close();	
				
	}

	private static void mergeListPatternClusters(List<List<String>> curr, List<List<String>> next) {
		next.get(1).removeAll(curr.get(1));
		curr.get(1).addAll(next.get(1));

	}

	private static boolean shouldmerge(List<List<String>> curr, List<List<String>> next) {
		if(curr.get(0).size() == next.get(0).size() && curr.get(0).containsAll(next.get(0)))
			return true;
		return false;
	}

}