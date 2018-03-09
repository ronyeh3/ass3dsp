import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



public class Step1Blessed5Gram {
	//total 5 gram records 247491358 100% after agregation

	private static HashSet<String> blessed = new HashSet<String>();
	public static class MapperClass5GramAgg extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private String valueAsString;
		private String[] splittedValue;
		private LongWritable occurences;
		Pattern p = Pattern.compile("[a-z]+");  // [a-zA-Z]+ change to lowecase

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			System.out.println("[Mapper Step1] INFO: Starting mapper setup.");
			URI [] cacheFilesURI = Job.getInstance(context.getConfiguration()).getCacheFiles();
			System.out.println("[Mapper Step1] INFO: entring setup");

			if((cacheFilesURI != null) && (cacheFilesURI.length >0)) {
				System.out.println("[Mapper Step1] INFO: In Setup adding CacheFiles URI to N1Map");
				for (URI cacheFileURI : cacheFilesURI) {
					if(cacheFileURI.getPath() != null) {
						System.out.println("[Mapper Step1] INFO: In Setup path to file is not null");

						Path cacheFilePath = new Path(cacheFileURI.getPath());
						try{

							String fileName = cacheFilePath.getName().toString();
							FileReader fr = new FileReader(fileName);
							readFile(fr , fileName);
							fr.close();
						}
						catch (IOException e) {
							System.err.println("[Mapper Step4]  ERROR: in opening cache file path.");
							e.printStackTrace();			
						}
					}
				}
			}
			else {
				System.err.println("[Mapper step4]  ERROR: cacheFile is Empty!!! ");
			}
			System.out.println("[Mapper Step4] INFO: Finished loading N1 File.");


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

			HashSet<String> currList;
			if (filename.substring(0, 5).equals("bless"))
				currList = blessed;

			else
				return;

			String line;
			while ((line = joinReader.readLine()) != null) {
				String keyValue = line.toString();
				currList.add(keyValue);  
			}

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			valueAsString = value.toString();
			splittedValue = valueAsString.split("\t");
			String blessedW1, blessedW2;
			String[] blessedW1W2Rel;
			String[] splittedNgram = splittedValue[0].trim().toLowerCase().split("\\s+");
			for (String word : splittedNgram)
				if (!p.matcher(word).matches()) { return; }
			for (String blessedPair : blessed) {
				blessedW1W2Rel = blessedPair.split("\t");
				blessedW1 = blessedW1W2Rel[0];
				blessedW2 = blessedW1W2Rel[1];
				if ((splittedNgram[1].equals(blessedW1) && splittedNgram[3].equals(blessedW2)) ||
						
						(splittedNgram[1].equals(blessedW2) && splittedNgram[3].equals(blessedW1))) {
					occurences = new LongWritable(Long.parseLong(splittedValue[1]));
					word.set(splittedValue[0]);  
					context.write(word, occurences);
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {



		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {      
		
			long sum = 0;
			for (LongWritable value : values)
				sum += value.get();			
			context.write(key, new LongWritable(sum));
		}
	}


	/*	args[0] =inputPath, s3a datasets path to 5 gram
	 * args[1] = output path */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		System.out.println("[~programer messege~] Staring step 1 .");

		Job job = Job.getInstance(conf);
		job.setJarByClass(Step1Blessed5Gram.class);
		job.setMapperClass(MapperClass5GramAgg.class);
		job.setReducerClass(ReducerClass.class);
		//job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		/*  cache  */
		FileSystem fs_s3a = new S3AFileSystem();
		//args[1] = "s3n://ass3dsp181resultstamir"
		fs_s3a.initialize(URI.create(args[1] ) , conf);
		// args[2] = "s3n://ass3dsp181resultstamir/blessed/"
		Path path = new Path(args[2]);
		RemoteIterator<LocatedFileStatus> itr = fs_s3a.listFiles(path, false);
		while (itr.hasNext()) {
			LocatedFileStatus f = itr.next();
			if(f.getPath().getName().toString().equals("_SUCCESS")) {
				System.out.println("[Step1] Skiped \"_SUCCESS\" file");
				continue;
			}
			System.out.println("[Step1] Adding "+ f.getPath().toUri()+"  URI to cache File");
			job.addCacheFile(f.getPath().toUri());
			System.out.println("[Step1]	A file has been added to cache");
		}
		fs_s3a.close();
		System.out.println("[Step1] Finished adding files to cache.");
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.waitForCompletion(true);
		System.out.println("finished step1 ");
	}
}
