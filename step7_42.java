import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;
import com.google.gson.JsonStreamParser;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import org.apache.hadoop.io.LongWritable;

/*
 *   input - step 1 :  5 gram minimize (to see that it a valide sentence)
 *   input ram - blessed words
 *   input ram - step 6_344: all clusters and how many
 */   


public class step7_42 {
	private static final float alpha = 1;
	static HashSet<String> blessed_words = new HashSet<String>();  //pair words and relation word [w1 w2 w3, ... ]
	static ArrayList<List<List<String>>> allClusters =  new ArrayList<List<List<String>>>(); 


	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		public static Gson gson = new Gson();
		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			System.out.println("entring setup");
			Path [] cacheFiles = context.getLocalCacheFiles();
			System.out.println("HOW MANY FILES?   "+cacheFiles.length);

			if((cacheFiles != null) && (cacheFiles.length >0)) {
				for (Path cacheFile : cacheFiles) {
					FSDataInputStream in = fs.open(cacheFile);
					readFile(in, cacheFile.getName());
					in.close();
				}
			}
			else {
				System.err.println("[Mapper step5]  Error cacheFile is Empty!!! ");
			}
			System.out.println("[step5 mapper] Finished loading cache Files.");

		}
		public void readFile(FSDataInputStream in, String filename) throws IOException {
			if (filename.substring(0, 4).equals("last")) {  // clusters
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				JsonReader reader = new JsonReader(br);
				Type type = new TypeToken<ArrayList<List<List<String>>>>(){}.getType();
				allClusters = gson.fromJson(reader, type);

			}
			else if (filename.substring(0, 5).equals("bless")) {//blessed
				BufferedReader joinReader = new BufferedReader(new InputStreamReader(in, "UTF8"));
				String line;
				while ((line = joinReader.readLine()) != null) {
					String words = line.toString();
					blessed_words.add(words);
				}

			}
		}
		
		public void map(LongWritable LongWritable, Text value, Context context) throws IOException,  InterruptedException {
			String ngram = value.toString().split("\t")[0];
			//the child went to school
			// check if "the went school" (x1 x3 x5) is a pattern
			// check if "child to" (x2 x4) is a blessed pair
			String[] splittedNgram = ngram.split("\\s+");
			String[] blessedPairArr; // blessed1 blessed2 relation
			String firstBlessed, secondBlessed;
			String currHit, totalHits="";
			String secondWordInNgram = splittedNgram[1].toLowerCase(); // x2
			String fourthWordInNgram = splittedNgram[3].toLowerCase(); // x4
			String ngramPattern = 
					splittedNgram[0] + " " + splittedNgram[2] + " "+ splittedNgram[4]; // x1 x3 x5
			
			if (ngramAppearsAsPattern(splittedNgram)) {   /// the ngram (x1 x3 x5) appears as pattern somewhere (don't know if confirmed/unconfirmed)
				for (String blessedPair : blessed_words) {
					blessedPairArr = blessedPair.split("\t");
					firstBlessed = blessedPairArr[0].toLowerCase();
					secondBlessed = blessedPairArr[1].toLowerCase();
					if ((secondWordInNgram.equals(firstBlessed) && fourthWordInNgram.equals(secondBlessed)) ||
							(secondWordInNgram.equals(secondBlessed) && fourthWordInNgram.equals(firstBlessed))) { /// The x2 x4 of ngram is a blessed pair
						//here we need to iterate through all clusters and calculate hits
						for (List<List<String>> cluster : allClusters) {
							currHit = getHits(cluster, ngramPattern);
							totalHits += currHit+" ";
						}
						context.write(new Text(blessedPair), new Text(totalHits));
						totalHits = "";
					}
				}
			}
		}


	}  
	private static String getHits(List<List<String>> cluster, String ngramPattern) {
		List<String> confirmed = cluster.get(0);
		List<String> unconfirmed = cluster.get(1);
		float n = confirmed.size();
		float m = unconfirmed.size();
		float appearsAsCore=0;
		float appearsAsUnconfirmed=0;
		for (String corePattern : confirmed) {
			corePattern = corePattern.toLowerCase();
			if (corePattern.equals(ngramPattern)) {
				appearsAsCore++;
			}
		}
		for (String unconfirmedPattern : unconfirmed) {
			unconfirmedPattern = unconfirmedPattern.toLowerCase();
			if (unconfirmedPattern.equals(ngramPattern)) {
				appearsAsUnconfirmed++;
			}
		}
		float firstValue, secondValue;
		firstValue = (n == 0) ? 0 : (appearsAsCore/n);
		secondValue = (m == 0) ? 0 : (appearsAsUnconfirmed/m);
		return Float.toString(  firstValue   +   (alpha*secondValue)   );


	}

	public static boolean ngramAppearsAsPattern(String[] ngram) {
		String ngram_w1w3w5_string = ngram[0] + " " + ngram[2] + " " + ngram[4]; // x1 x3 x5
		for (List<List<String>> currList : allClusters) { // currList is a cluster
			for (List<String> currList1 : currList) {
				for (String pattern : currList1) {
					if (pattern.equals(ngram_w1w3w5_string)) {
						return true;
					}

				}
			}
		}
		return false;
	}


	public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
		// reducer receives blessed word couple (key) and their probabilities per cluster (value)
		// (blessed1 blessed2 ; connection) , (0.0 0.6 0.2 0.4 0.5 ......... )
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			float[] hitsVector = null;
			String[] vectorData;
			float currElement;
			for (Text value : values) {
				vectorData = value.toString().split(" ");
				if (hitsVector == null)
					hitsVector = new float[vectorData.length];
				for (int i=0 ; i< vectorData.length ; i++) {
					currElement = Float.parseFloat(vectorData[i]);
					hitsVector[i] += currElement;            /// why array out of bound here ???
				}
			}
			String finalVector="";
			for (float value : hitsVector) {
				finalVector += value + ",";
			}
			String actualKey = key.toString().split("\t")[2];
			actualKey = finalVector+actualKey;
			key = new Text(actualKey);
			context.write(key, null);


			//context.write(key, new Text(finalVector));
			// now: blessed1 blessed2 relation	0.5,0.2,1,13
			// desired: 0.5,0.2,1,13,relation

		}


	}



	public static void main(String[] args) throws Exception {

		System.load("C:/Users/Tamir/Desktop/lzo2.dll");
		System.setProperty("hadoop.home.dir", "C:/hadoop-2.6.2");

		//			System.load("C:/Users/RONlptp/eclipse-workspace/ass2localRunner/lib/lzo2.dll");
		//			System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.2");

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(step7_42.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);   //input 5gram!!!!!!!
		MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class,
				Text.class, Text.class);
		Path bllessedAndClusters = new Path(args[1]+"[^_]*");  //!!!!!!!!!! input cache
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] list = fs.globStatus(bllessedAndClusters);  //TODO s3 is object 
		for (FileStatus status : list) {
			job.addCacheFile(status.getPath().toUri());
		}
		FileOutputFormat.setOutputPath(job, new Path(args[2]));		  
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

