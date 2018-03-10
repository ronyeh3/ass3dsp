import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;

/*
 * input - step 1 :  5 gram just the words
 * input - step 3 (cache) :  the classifications of the HFW and hooks and notcwd
 * 
 * output: byhook , by pattern
 */
//TODO ADD RESTRICRING OF CW. Target word should have less occurences than Fc!!!!!!!

public class Step4 {
	static HashSet<String> hfw = new HashSet<String>();  // onegram!!!!!!!!!!
	static HashSet<String> hooks = new HashSet<String>();
	static HashSet<String> notcws = new HashSet<String>(); // there are less not CWs than CWs, working with the "not" for efficiency
	// because we need them just to make sure we don't choose target words that are very common (is, the ...)


	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		private static final String type1 = "-1-";
		private static final String type2 = "-2-";


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

			HashSet<String> currList;
			if (filename.substring(0, 3).equals("hfw"))
				currList = hfw;
			else if (filename.substring(0, 3).equals("hoo")) //hook
				currList = hooks;
			else if (filename.substring(0, 3).equals("not"))  //not-content-word
				currList = notcws;
			else
				return;

			String line;
			while ((line = joinReader.readLine()) != null) {
				String keyValue = line.toString();
				currList.add(keyValue);  
			}

		}


		public void map(LongWritable LongWritable, Text value, Context context) throws IOException,  InterruptedException {
			String ngram = value.toString().split("\t")[0];
			String[] ngramWords = ngram.split("\\s+");
			String target, pattern, hookword;
			//System.out.println("Current ngram: "+ngram);
			if (hfw.contains(ngramWords[0]) && hfw.contains(ngramWords[2]) && hfw.contains(ngramWords[4])){
				if (hooks.contains(ngramWords[1])) {  
					if (!notcws.contains(ngramWords[3])) {// //[3]<fc - so it is content word
						hookword = ngramWords[1];
						target = ngramWords[3];  // not is - contain 
						pattern = ngramWords[0] + " " + ngramWords[2] + " " + ngramWords[4];
						context.write(new Text(type1+"\t"+hookword), new Text(pattern+"##"+target));
						context.write(new Text(type2+"\t"+pattern), new Text(hookword));
					}
				}
				else if (hooks.contains(ngramWords[3])) {   //maybe not else-, another if
					if (!notcws.contains(ngramWords[1])) {

						hookword = ngramWords[3];
						target = ngramWords[1];
						pattern = ngramWords[0] + " " + ngramWords[2] + " " + ngramWords[4];
						context.write(new Text(type1+"\t"+hookword), new Text(pattern+"##"+target));
						context.write(new Text(type2+"\t"+pattern), new Text(hookword));
					}
				}

			}  
		}
	}

	public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
		private MultipleOutputs<Text,Text> mos;

		public void setup(Context context) {
			mos = new MultipleOutputs<Text,Text>(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {

			//no Memory assumption , maybe we cant store all pattern and target for a hook
			// we output the values one by one for a hook word
			//but we must assumes this because in step 6 we put in memory all the patterns target anyway
			if (key.toString().charAt(1) == '1') {
				String actualKey = key.toString().split("\t")[1];
				for (Text patternsAndTargets : values) 
					mos.write("byHook", new Text(actualKey), new Text(patternsAndTargets));
				//				mos.write("byHook", new Text(""), null);
			}
			else { // if (key.toString().charAt(1) == '2')

				String actualKey = key.toString().split("\t")[1];
				for (Text hookWords : values) 
					mos.write("byPattern", new Text(actualKey), new Text(hookWords));
				//				mos.write("byPattern", new Text(""), null);
			}
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
		job.setJarByClass(Step4.class);
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

		MultipleOutputs.addNamedOutput(job, "byHook", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "byPattern", TextOutputFormat.class,
				Text.class, Text.class);
		Path hfwAndHooksAndNotCWD = new Path(args[1]+"[^_]*");  //!!!!!!!!!! input
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] list = fs.globStatus(hfwAndHooksAndNotCWD);  
		for (FileStatus status : list) {
			job.addCacheFile(status.getPath().toUri());
			System.out.println("--CACHE: Added: "+status.getPath().toUri().toString());
		}
		FileOutputFormat.setOutputPath(job, new Path(args[2]));		  
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
