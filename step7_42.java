import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
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
 *   input - step 1 :  5 gram minimize
 *   input ram - blessed words
 *   input ram - step 6_344: all clusters and how many
 */   


public class step7_42 {
	static List<String> blesed_words = new ArrayList<String>();  //pair words and relation word [w1 w2 w3, ... ]
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
			if (filename.substring(0, 3).equals("las")) {  // clusters
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				JsonReader reader = new JsonReader(br);
				Type type = new TypeToken<ArrayList<List<List<String>>>>(){}.getType();
				allClusters = gson.fromJson(reader, type);

			}
			else {//bslessed
				BufferedReader joinReader = new BufferedReader(new InputStreamReader(in, "UTF8"));
				String line;
				while ((line = joinReader.readLine()) != null) {
					String words = line.toString();
					blesed_words.add(words);
				}

			}
		}
		public void map(LongWritable LongWritable, Text value, Context context) throws IOException,  InterruptedException {
			String ngram = value.toString();
			String[] splittedNgram = ngram.split("\\s+");
			String[] blessedPairArr;
			String firstBlessed, secondBlessed;
			String currHit, totalHits="";             
			if (ngramAppearsAsPattern(splittedNgram)) {   ///////// the ngram (x1 x3 x5) appears as pattern somewhere (don't know if confirmed/unconfirmed)
				for (String blessedPair : blesed_words) {
					blessedPairArr = blessedPair.split("\\t");
					firstBlessed = blessedPairArr[0].toLowerCase();
					secondBlessed = blessedPairArr[1].toLowerCase();
					if (splittedNgram[1].toLowerCase().equals(firstBlessed) && splittedNgram[3].toLowerCase().equals(secondBlessed)) { /// The x2 x4 of ngram is a blessed pair

						//here we need to iterate through all clusters and calculate hits
						for (List<List<String>> cluster : allClusters) {
							currHit = getHits(cluster, blessedPairArr);
							totalHits += " "+currHit;
						}
						System.out.println("NOW EMIT!!!!\n "+blessedPair+" ## \n"+totalHits);
						context.write(new Text(blessedPair), new Text(totalHits));
					}
				}
			}
		}


	}  
	private static String getHits(List<List<String>> cluster, String[] blessed) {
		List<String> confirmed = cluster.get(0);
		List<String> unconfirmed = cluster.get(1);
		System.out.println("Confirmed: "+confirmed.toString());
		System.out.println("Unconfirmed: "+unconfirmed.toString());
		float alpha = 1;
		float n = confirmed.size();
		float m = unconfirmed.size();
		float appearsAsCore=0;
		float appearsAsUnconfirmed=0;
		for (String corePattern : confirmed) {
			corePattern = corePattern.toLowerCase();
			if (corePattern.contains(blessed[0].toLowerCase()) && corePattern.contains(blessed[1].toLowerCase())) {
				appearsAsCore++;
			}
		}
		for (String unconfirmedPattern : unconfirmed) {
			unconfirmedPattern = unconfirmedPattern.toLowerCase();
			if (unconfirmedPattern.contains(blessed[0].toLowerCase()) && unconfirmedPattern.contains(blessed[1].toLowerCase())) {
				appearsAsUnconfirmed++;
			}
		}
		float firstValue, secondValue;
		firstValue = (n == 0) ? 0 : (appearsAsCore/n);
		secondValue = (m == 0) ? 0 : (appearsAsUnconfirmed/m);
		return Float.toString(  firstValue   +   (alpha*secondValue)   );


	}

	public static boolean ngramAppearsAsPattern(String[] ngram) {
		String ngram_w1w3w5_string = ngram[0] + " " + ngram[2] + " " + ngram[4]; // x1 x2 x3
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
		private MultipleOutputs<Text,Text> mos;
		// reducer receives blessed word couple (key) and their probabilities per cluster (value)
		// (blessed1 blessed2 ; connection) , (0.0 0.6 0.2 0.4 0.5 ......... )
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			float[] hitsVector=null;
			String[] vectorData;
			float currElement;
			for (Text value : values) {
				while (value.toString().charAt(0) == ' ') {
					value = new Text(value.toString().substring(1, value.toString().length()));
				}
				vectorData = value.toString().split(" ");
				if (hitsVector == null)
					hitsVector = new float[vectorData.length];
				for (int i=0 ; i< vectorData.length ; i++) {
					System.out.println("Vector data "+i+": "+vectorData[i]);
					currElement = Float.parseFloat(vectorData[i]);
					hitsVector[i] += currElement;
				}
			}
			String finalVector="";
			for (float value : hitsVector) {
				finalVector += value + " ";
			}
			context.write(key, new Text(finalVector));

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
}
