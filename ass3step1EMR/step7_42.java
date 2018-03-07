import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
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
 *   ## MapReduce Application ##
 *   input - step 1 :  5 gram minimize (to see that it a valide sentence)
 *   input ram - blessed words
 *   input ram - step 6_344: all clusters and how many
 */   


public class step7_42 {
	static List<String> blessed_words = new ArrayList<String>();  //pair words and relation word [w1 w2 w3, ... ]
	static ArrayList<List<List<String>>> allClusters =  new ArrayList<List<List<String>>>(); 


	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		public static Gson gson = new Gson();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("[Mapper Step7] INFO: Starting mapper setup.");
			URI [] cacheFilesURI = Job.getInstance(context.getConfiguration()).getCacheFiles();
			System.out.println("[Mapper Step7] INFO: entring setup");

			if((cacheFilesURI != null) && (cacheFilesURI.length >0)) {
				System.out.println("[Mapper Step7] INFO: In Setup adding CacheFiles URI to N1Map");
				for (URI cacheFileURI : cacheFilesURI) {
					if(cacheFileURI.getPath() != null) {
						System.out.println("[Mapper Step7] INFO: In Setup path to file is not null");

						Path cacheFilePath = new Path(cacheFileURI.getPath());
						try{

							String fileName = cacheFilePath.getName().toString();
							FileReader fr = new FileReader(fileName);
							readFile(fr , fileName);
							fr.close();
						}
						catch (IOException e) {
							System.err.println("[Mapper Step7]  ERROR: in opening cache file path.");
							e.printStackTrace();			
						}
					}
				}
			}
			else {
				System.err.println("[Mapper Step7]  ERROR: cacheFile is Empty!!! ");
			}
			System.out.println("[Mapper Step7] INFO: Finished loading N1 File.");


		}
		public void readFile(FileReader in, String filename) throws IOException {

			System.out.println("reading a file ...");

			BufferedReader joinReader = null;
			try {
				joinReader = new BufferedReader(in);
			}
			catch (Exception e) {
				System.err.println("[Mapper Step7]  ERROR in readFile. failed in BufferedReader");
				e.printStackTrace();
			}

			if (filename.substring(0, 4).equals("last")) {  // clusters from step_6_44
				JsonReader reader = new JsonReader(joinReader); // clusters file is in json format
				Type type = new TypeToken<ArrayList<List<List<String>>>>(){}.getType();
				allClusters = gson.fromJson(reader, type);
				reader.close();

			}
			else if (filename.substring(0, 5).equals("bless")){ // blessed words
				String line;
				while ((line = joinReader.readLine()) != null) {
					String words = line.toString();
					blessed_words.add(words);
				}

			}
			joinReader.close();
		}

		@Override
		public void map(LongWritable LongWritable, Text value, Context context) throws IOException,  InterruptedException {
			String ngram = value.toString();
			String[] splittedNgram = ngram.split("\\s+");
			String[] blessedPairArr;
			String firstBlessed, secondBlessed, secondWordInNgram, fourthWordInNgram;
			String currHit, totalHits="";             
			if (ngramAppearsAsPattern(splittedNgram)) {   ///////// the ngram (x1 x3 x5) appears as pattern somewhere (don't know if confirmed/unconfirmed)
				for (String blessedPair : blessed_words) {
					blessedPairArr = blessedPair.split("\t");
					firstBlessed = blessedPairArr[0].toLowerCase();
					secondBlessed = blessedPairArr[1].toLowerCase();
					secondWordInNgram = splittedNgram[1].toLowerCase();
					fourthWordInNgram = splittedNgram[3].toLowerCase();
					if ((secondWordInNgram.equals(firstBlessed) && fourthWordInNgram.equals(secondBlessed)) ||
							(secondWordInNgram.equals(secondBlessed) && fourthWordInNgram.equals(firstBlessed))) { /// The x2 x4 of ngram is a blessed pair

						//here we need to iterate through all clusters and calculate hits
						for (List<List<String>> cluster : allClusters) {
							currHit = getHits(cluster, blessedPairArr);
							totalHits += currHit+" ";
						}
						context.write(new Text(blessedPair), new Text(totalHits));
						totalHits = "";
					}
				}
			}
		}


	}  
	private static String getHits(List<List<String>> cluster, String[] blessed) {
		List<String> confirmed = cluster.get(0);
		List<String> unconfirmed = cluster.get(1);
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


		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
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
		FileInputFormat.addInputPaths(job, args[0]);   //input 5gram

		/*  cache  */
		FileSystem fs_s3a = new S3AFileSystem();
		// args[1] = "s3n://ass3dsp181resultstamir"
		fs_s3a.initialize(URI.create(args[1] ) , conf);
		// args[2] = "s3n://ass3dsp181resultstamir/step6_34_4ResultAndBlessed/"
		// manually upload the input to S3. (it was created locally and not as MapReduce application)
		// *blessed words* are also found in this folder (step6_34_4ResultAndBlessed)
		Path path = new Path(args[2]);
		RemoteIterator<LocatedFileStatus> itr = fs_s3a.listFiles(path, false);
		while (itr.hasNext()) {
			LocatedFileStatus f = itr.next();
			if(f.getPath().getName().toString().equals("_SUCCESS")) {
				System.out.println("[Step7] Skiped \"_SUCCESS\" file");
				continue;
			}
			System.out.println("[Step7] Adding "+ f.getPath().toUri()+"  URI to cache File");
			job.addCacheFile(f.getPath().toUri());
			System.out.println("[Step7]	A file has been added to cache");
		}
		fs_s3a.close();
		System.out.println("[Step7] Finished adding files to cache.");

		FileOutputFormat.setOutputPath(job, new Path(args[3]));		//output folder  
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

