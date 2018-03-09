import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
 
/* 
 * 		Map input records=24492478978
		Map output records=10663205018
		Map output bytes=364806084770
		Map output materialized bytes=38565333207
		Input split bytes=971056
		Combine input records=0
		Combine output records=0
		Reduce input groups=247491358
		Reduce shuffle bytes=38565333207
		Reduce input records=10663205018
		Reduce output records=247491358
 * 
 */


public class Step1 {
//total 5 gram records 247491358 100% after agregation
	public static class MapperClass5GramAgg extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private String valueAsString;
		private String[] splittedValue;
		private LongWritable occurences;
		Pattern p = Pattern.compile("[a-z]+");  // [a-zA-Z]+ change to lowecase
		
		private static final int limiter = 5; //for 20%
		private static  int counter = 0;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			
			valueAsString = value.toString();
			splittedValue = valueAsString.split("\t");
			String[] splittedNgram = splittedValue[0].trim().toLowerCase().split("\\s+");
			for (String word : splittedNgram)
				if (!p.matcher(word).matches()) { return; }
			
			occurences = new LongWritable(Long.parseLong(splittedValue[1]));
			word.set(splittedValue[0]);  
			context.write(word, occurences);
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
		job.setJarByClass(Step1.class);
		job.setMapperClass(MapperClass5GramAgg.class);
        job.setReducerClass(ReducerClass.class);
        //job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("finished step1 ");
	  }
}
