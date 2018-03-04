import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;

/*
 * input - step 1 :  5 gram just the words
 * input - step 3 (cache) :  the classifications of the HFW and hooks
 */
//TODO ADD RESTRICRING OF CW. Target word should have less occurences than Fc!!!!!!!

public class Step4 {
	static List<String> hfw = new ArrayList<String>();  // onegram!!!!!!!!!!
	static List<String> hooks = new ArrayList<String>();


	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		private static final String type1 = "-1-";
		private static final String type2 = "-2-";
		
		
		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("[Mapper Step5] INFO: Starting mapper setup.");
			URI [] cacheFilesURI = Job.getInstance(context.getConfiguration()).getCacheFiles();
			System.out.println("[Mapper Step5] INFO: entring setup");

			if((cacheFilesURI != null) && (cacheFilesURI.length >0)) {
				System.out.println("[Mapper Step5] INFO: In Setup adding CacheFiles URI to N1Map");
				for (URI cacheFileURI : cacheFilesURI) {
					if(cacheFileURI.getPath() != null) {
						System.out.println("[Mapper Step5] INFO: In Setup path to file is not null");

						Path cacheFilePath = new Path(cacheFileURI.getPath());
						try{

							String fileName = cacheFilePath.getName().toString();
							FileReader fr = new FileReader(fileName);
							readFile(fr , fileName);
							fr.close();
						}
						catch (IOException e) {
							System.err.println("[Mapper Step5]  ERROR: in opening cache file path.");
							e.printStackTrace();			
						}
					}
				}
			}
			else {
				System.err.println("[Mapper step5]  ERROR: cacheFile is Empty!!! ");
			}
			System.out.println("[Mapper Step5] INFO: Finished loading N1 File.");


		}
		public void readFile(FileReader in, String filename) throws IOException {
			
			System.out.println("reading a file ...");

			BufferedReader joinReader = null;
			try {
				joinReader = new BufferedReader(in);
			}
			catch (Exception e) {
				System.err.println("[Mapper step5]  ERROR in readFile. failed in BufferedReader");
				e.printStackTrace();
			}
			
			List<String> currList;
			if (filename.substring(0, 3).equals("hfw"))
				currList = hfw;
			else
				currList = hooks;

			String line;
			while ((line = joinReader.readLine()) != null) {
				String keyValue = line.toString();
				currList.add(keyValue);  
			}

		}

		public void map(LongWritable LongWritable, Text value, Context context) throws IOException,  InterruptedException {
			String ngram = value.toString();

			String[] ngramWords = ngram.split("\\s+");
			String target, pattern, hookword;
			if (hfw.contains(ngramWords[0]) && hfw.contains(ngramWords[2]) && hfw.contains(ngramWords[4])){
				if (hooks.contains(ngramWords[1])) {   //[3]<fc
					hookword = ngramWords[1];
					target = ngramWords[3];  // not is - contain 
					pattern = ngramWords[0] + " " + ngramWords[2] + " " + ngramWords[4];
					context.write(new Text(type1+"\t"+hookword), new Text(pattern+"##"+target));
					context.write(new Text(type2+"\t"+pattern), new Text(hookword));
				}
				else if (hooks.contains(ngramWords[3])) {
					hookword = ngramWords[3];
					target = ngramWords[1];
					pattern = ngramWords[0] + " " + ngramWords[2] + " " + ngramWords[4];
					context.write(new Text(type1+"\t"+hookword), new Text(pattern+"##"+target));
					context.write(new Text(type2+"\t"+pattern), new Text(hookword));

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
			if (key.toString().charAt(1) == '1') {
				String patternsAndTargets="";
				String actualKey = key.toString().split("\t")[1];
				for (Text value : values) {
					patternsAndTargets += value.toString()+"|";
				}
				patternsAndTargets = patternsAndTargets.substring(0,patternsAndTargets.length());
				mos.write("byHook", new Text(actualKey), new Text(patternsAndTargets));
				mos.write("byHook", new Text(""), null);
			}
			else { // if (key.toString().charAt(1) == '2')
				String hookWords="";
				String actualKey = key.toString().split("\t")[1];
				for (Text value : values) {
					hookWords += value.toString()+"|";
				}
				hookWords = hookWords.substring(0,hookWords.length());
				mos.write("byPattern", new Text(actualKey), new Text(hookWords));
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
		FileInputFormat.addInputPaths(job, args[0]);   //input 5 gram!!!!!!!
		
		/*  cache  */
		FileSystem fs_s3a = new S3AFileSystem();
		//args[1] = "s3a://onegramoutputresult"
		fs_s3a.initialize(URI.create(args[1] ) , conf);
		// args[2] = "s3a://onegramoutputresult/step1Result"
		Path path = new Path(args[2]);
		RemoteIterator<LocatedFileStatus> itr = fs_s3a.listFiles(path, false);
		while (itr.hasNext()) {
			LocatedFileStatus f = itr.next();
			if(f.getPath().getName().toString().equals("_SUCCESS")) {
				System.out.println("[Step5] Skiped \"_SUCCESS\" file");
				continue;
			}
			System.out.println("[Step5] Adding "+ f.getPath().toUri()+"  URI to cache File");
			job.addCacheFile(f.getPath().toUri());
			System.out.println("[Step5]	A file has been added to cache");
		}
		fs_s3a.close();
		System.out.println("[Step5] Finished adding files to cache.");
		
		
		MultipleOutputs.addNamedOutput(job, "byHook", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "byPattern", TextOutputFormat.class,
				Text.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));		//output path  
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
