import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
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
			if((cacheFiles != null) && (cacheFiles.length >0)) {
				for (Path cacheFile : cacheFiles) {
					if (cacheFile.getName().charAt(0) == 'h') {
						FSDataInputStream in = fs.open(cacheFile);
						readFile(in, cacheFile.getName());
						in.close();
					}
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
				JsonStreamParser jp = new JsonStreamParser(br);
				Type type = new TypeToken<ArrayList<List<String>>>(){}.getType();
				while(jp.hasNext()) {
					allClusters.add(gson.fromJson(jp.next(), type));
				}
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
			for (String blessedPair : blesed_words) {
				blessedPairArr = blessedPair.split("\\t");
				firstBlessed = blessedPairArr[0];
				secondBlessed = blessedPairArr[1];
				if (splittedNgram[1].equals(firstBlessed) && splittedNgram[3].equals(secondBlessed)) { 
					if (ngramHasBlessedAndPattern(splittedNgram)) {

						//here we need to iterate through all clusters and calculate hits
						for (List<List<String>> currList : allClusters) {
							for (List<String> currPatternList : currList) {
								currHit = getHits(currPatternList, blessedPairArr);
								totalHits += " "+currHit;
							}

						}
					}
				}
				context.write(new Text(blessedPair), new Text(totalHits));
			}



			//			String[] ngramWords = ngram.split("\\s+");
			//			String target, pattern, hookword;
			//			if (hfw.contains(ngramWords[0]) && hfw.contains(ngramWords[2]) && hfw.contains(ngramWords[4])){
			//				if (hooks.contains(ngramWords[1])) {
			//					hookword = ngramWords[1];
			//					target = ngramWords[3];  // not is - contain 
			//					pattern = ngramWords[0] + " " + ngramWords[2] + " " + ngramWords[4];
			//					context.write(new Text(type1+"\t"+hookword), new Text(pattern+"##"+target));
			//					context.write(new Text(type2+"\t"+pattern), new Text(hookword));
			//				}
			//				else if (hooks.contains(ngramWords[3])) {
			//					hookword = ngramWords[3];
			//					target = ngramWords[1];
			//					pattern = ngramWords[0] + " " + ngramWords[2] + " " + ngramWords[4];
			//					context.write(new Text(type1+"\t"+hookword), new Text(pattern+"##"+target));
			//					context.write(new Text(type2+"\t"+pattern), new Text(hookword));
			//
			//				}


		}  
		private String getHits(List<String> currPatternList, String[] blessedPairArr) {
			return null;

		}
		public boolean ngramHasBlessedAndPattern(String[] ngram) {
			String firstBlessed, secondBlessed;
			String[] blessedPairArr;
			String ngram_w1w3w5_string = ngram[0] + " " + ngram[2] + " " + ngram[4];
			for (List<List<String>> currList : allClusters) {
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

			public void setup(Context context) {
				mos = new MultipleOutputs<Text,Text>(context);
			}

			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
				//			if (key.toString().charAt(1) == '1') {
				//				String patternsAndTargets="";
				//				String actualKey = key.toString().split("\t")[1];
				//				for (Text value : values) {
				//					patternsAndTargets += value.toString()+"|";
				//				}
				//				patternsAndTargets = patternsAndTargets.substring(0,patternsAndTargets.length());
				//				mos.write("byHook", new Text(actualKey), new Text(patternsAndTargets));
				//				mos.write("byHook", new Text(""), null);
				//			}
				//			else { // if (key.toString().charAt(1) == '2')
				//				String hookWords="";
				//				String actualKey = key.toString().split("\t")[1];
				//				for (Text value : values) {
				//					hookWords += value.toString()+"|";
				//				}
				//				hookWords = hookWords.substring(0,hookWords.length());
				//				mos.write("byPattern", new Text(actualKey), new Text(hookWords));
				//				mos.write("byPattern", new Text(""), null);
				//			}
				//		}
				//		public void cleanup(Context context) throws IOException {
				//			try {
				//				mos.close();
				//			} catch (InterruptedException e) {
				//				e.printStackTrace();
				//			}
			}


		}

		public static void main(String[] args) throws Exception {

			//		System.load("C:/Users/Tamir/Desktop/lzo2.dll");
			//		System.setProperty("hadoop.home.dir", "C:/hadoop-2.6.2");

			System.load("C:/Users/RONlptp/eclipse-workspace/ass2localRunner/lib/lzo2.dll");
			System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.2");

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
