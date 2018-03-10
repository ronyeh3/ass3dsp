import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.LongWritable;

/*
 * input - step 1 : step4 output
 * input - step 3 (cache) :  one gram , 2 gram
 * 
 * output: byhook , by pattern
 */

public class Step4PMI {

	static Map <String, String> oneGramMap = new ConcurrentHashMap<String, String>();
	static Map <String, String> twoGramMap = new ConcurrentHashMap<String, String>();
	public static long NumberOfWords;// = 193863291517430L 301888295413;


	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			System.out.println("entring setup");
			Path [] cacheFiles = context.getLocalCacheFiles();
			if((cacheFiles != null) && (cacheFiles.length >0)) {
				for (Path cacheFile : cacheFiles) {
					if (cacheFile.getName().charAt(0) == 'h') {   //hfw or hook
						FSDataInputStream in = fs.open(cacheFile);
						readFile(in, cacheFile.getName());
						in.close();
					}
				}
			}
			else {
				System.err.println("[Mapper step5]  ERROR: cacheFile is Empty!!! ");
			}
			System.out.println("[Mapper Step5] INFO: Finished loading N1 File.");


		}
		public void readFile(InputStream in, String filename) throws IOException {

			System.out.println("reading a file ...");

			BufferedReader joinReader = new BufferedReader(new InputStreamReader(in, "UTF8"));


			Map<String, String> currList;
			if (filename.substring(0, 5).equals("1gram"))
				currList = oneGramMap;
			else if (filename.substring(0, 5).equals("2gram")) //
				currList = twoGramMap;
			else
				return;

			String line;
			while ((line = joinReader.readLine()) != null) {
				String[] keyValue = line.toString().split("\t");
				currList.put(keyValue[0].trim(), keyValue[1].trim());		
			}

		}

		protected double getPMI (String w1 , String w2) {
			String w1w2 = (w1.compareTo(w2) >0 ? w2 + " " +w1 : w1 + " " + w2  );

			double c_w1w2 =  (twoGramMap.containsKey(w1w2)? Math.log(Long.parseLong(twoGramMap.get(w1w2))): 0);

			return  c_w1w2 	+ Math.log(NumberOfWords) 
			- Math.log(Long.parseLong(oneGramMap.get(w1)))
			- Math.log(Long.parseLong(oneGramMap.get(w2)));
		}


		public void map(LongWritable LongWritable, Text value, Context context) throws IOException,  InterruptedException {
			String[] hook_patt_tar = value.toString().split("\t");
			String[] patt_tar = hook_patt_tar[1].split("##");

			double curpmi = getPMI(hook_patt_tar[0], patt_tar[1]);  // hook , target


			context.write( new Text(hook_patt_tar[0]), new Text(patt_tar[0]+"#"+patt_tar[1]+"#"+String.valueOf(curpmi))); //hook , 
		}




	}

	public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
		private MultipleOutputs<Text,Text> mos;

		HashMap<String , Pair<Double , ArrayList<String>>> cachearreylist = new HashMap<String , Pair<Double , ArrayList<String>>>();

		public void setup(Context context) {
			mos = new MultipleOutputs<Text,Text>(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {

			for (Text value : values) {
				String[] pat_tar_pmi =  value.toString().split("#");
				if (cachearreylist.containsKey(pat_tar_pmi[1]))
					cachearreylist.get(value).getSecond().add(pat_tar_pmi[0]);
				else {
					ArrayList<String> t = new ArrayList<String>();
					t.add(pat_tar_pmi[1]);
					Pair<Double , ArrayList<String>> ne = new Pair<Double , ArrayList<String>>(Double.parseDouble(pat_tar_pmi[2]),t);

				}
			}
			Map<String , Pair<Double , ArrayList<String>>> sortedMap =  getMapSortedByPMI(cachearreylist);

			double lowbound = (double)1/3;;
			int fromIndex = (int) (sortedMap.size() * lowbound) ;
			int toIndex = sortedMap.size() - fromIndex;

			int i =0;
			for (Entry<String, Pair<Double, ArrayList<String>>> str : sortedMap.entrySet()) {
				if(i<fromIndex) continue;
				for ( String r : str.getValue().getSecond()) {
					mos.write("byHook", key, new Text(r+"#"+str.getKey()));
				}
				if(i<toIndex) break;


			}
		}

		public static Map<String, Pair<Double , ArrayList<String>>> getMapSortedByPMI(final Map<String, Pair<Double , ArrayList<String>>> map) {
			return map.entrySet().stream()
					.sorted((e1, e2) ->Double.compare( e1.getValue().getFirst().doubleValue() , e2.getValue().getFirst().doubleValue()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
		}

		public void cleanup(Context context) throws IOException {
			try {
				mos.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}


	}



	public static void main(String[] args) throws Exception {

		System.load("C:/Users/Tamir/Desktop/lzo2.dll");
		System.setProperty("hadoop.home.dir", "C:/hadoop-2.6.2");

		//		System.load("C:/Users/RONlptp/eclipse-workspace/ass2localRunner/lib/lzo2.dll");
		//		System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.2");

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Step4PMI.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);   //input !!!!!!!

		MultipleOutputs.addNamedOutput(job, "byPattern", TextOutputFormat.class,
				Text.class, Text.class);
		Path wordCounts1gram = new Path(args[1]+"[^_]*");  //!!!!!!!!!! input
		FileSystem onegramFS = FileSystem.get(job.getConfiguration());
		FileStatus[] onegramList = onegramFS.globStatus(wordCounts1gram);  
		for (FileStatus status : onegramList) {
			job.addCacheFile(status.getPath().toUri());
			System.out.println("--CACHE: Added: "+status.getPath().toUri().toString());
		}
		Path wordCounts2gram = new Path(args[2]+"[^_]*");  //!!!!!!!!!! input
		FileSystem twogramFS = FileSystem.get(job.getConfiguration());
		FileStatus[] twogramList = twogramFS.globStatus(wordCounts2gram);  
		for (FileStatus status : twogramList) {
			job.addCacheFile(status.getPath().toUri());
			System.out.println("--CACHE: Added: "+status.getPath().toUri().toString());
		}
		FileOutputFormat.setOutputPath(job, new Path(args[3]));		  
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
