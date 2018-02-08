import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.io.LongWritable;


public class Step5 {
	
	
	static List<HookGroup> hookGroups = new ArrayList<HookGroup>(); // LIST OF CORPUSES


	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			System.out.println("entring setup");
			Path [] cacheFiles = context.getLocalCacheFiles();
			if((cacheFiles != null) && (cacheFiles.length >0)) {
				for (Path cacheFile : cacheFiles) {
					if (cacheFile.getName().substring(0, 3).equals("byH")) {
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
			
			BufferedReader joinReader = new BufferedReader(new InputStreamReader(in, "UTF8"));
			String line;
			String[] splittedLine, splittedPatternAndTarget;
			String hookWord, pattern, target;
			while ((line = joinReader.readLine()) != null) {
				//lap	sat my and##on|on last of##the|hands his and##in|living the of##in|hands her and##in|head his and##in|head her and##in|took victory around##a|sitting the of##on|sit my and##on|down his and##on|me his and##on|
				splittedLine = line.split("\t");
				hookWord = splittedLine[0];
				HookGroup currentHook = new HookGroup(hookWord);
				for (String patternAndTarget : splittedLine[1].split("\\|")) {
					splittedPatternAndTarget = patternAndTarget.split("##");
					pattern = splittedPatternAndTarget[0];
					Pattern patternObj = new Pattern(pattern,false,false);
					target = splittedPatternAndTarget[1];
					currentHook.addPatternToTarget(target, patternObj);
				}
				hookGroups.add(currentHook);  

			}

		}

		public void map(LongWritable LongWritable, Text value, Context context) throws IOException,  InterruptedException {

			context.write("", "");

			}  
		}

	public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
		private MultipleOutputs<Text,Text> mos;
		
		public void setup(Context context) {
			mos = new MultipleOutputs<Text,Text>(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			if (key.toString().charAt(1) == '1') {
				String patternsAndTargets="";
				String actualKey = key.toString().split("\t")[1];
				for (Text value : values) {
					patternsAndTargets += value.toString()+"|";
				}
				patternsAndTargets = patternsAndTargets.substring(0,patternsAndTargets.length());
				mos.write("byHook", new Text(actualKey), new Text("{"+patternsAndTargets+"}"));
				mos.write("byHook", new Text(""), null);
			}
			else { // if (key.toString().charAt(1) == '2')
				String hookWords="";
				String actualKey = key.toString().split("\t")[1];
				for (Text value : values) {
					hookWords += value.toString()+"|";
				}
				hookWords = hookWords.substring(0,hookWords.length());
				mos.write("byPattern", new Text(actualKey), new Text("{"+hookWords+"}"));
				mos.write("byPattern", new Text(""), null);
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
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Step5.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);

		Path hfwAndHooks = new Path("output/step4/[^_]*");
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] list = fs.globStatus(hfwAndHooks);  //TODO s3 is object 
		for (FileStatus status : list) {
			job.addCacheFile(status.getPath().toUri());
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		  
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
