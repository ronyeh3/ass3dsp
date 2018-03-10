import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

/*
Map input records=472764897
		Map output records=396499851
		Map output bytes=6646705702
		Map output materialized bytes=1287150417
		Input split bytes=21235
		Combine input records=0
		Combine output records=0
		Reduce input groups=4423745
		Reduce shuffle bytes=1287150417
		Reduce input records=396499851
		Reduce output records=4423745
		Spilled Records=792999702
 * 
 */

public class Step2 {
     
	// This step is a simple WordCount for the words that appear in 1gram.
	//output records - 4423745
	//new : input - 1gram
	public static class MapperClassWordCounter1Gram extends Mapper<LongWritable, Text, Text, LongWritable> {
	
		Pattern p = Pattern.compile("[a-z]+");		
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			String[] splitted = value.toString().split("\t");
		       if (splitted.length < 4) { return; } /* malformed line, skip it. */
		       String ngram = splitted[0].trim().toLowerCase();	//ngram to lower case !!!
		       String count = splitted[2];	//occurrences 
		       
		       String[] w1 = ngram.split("\\s+");
		       for (String w : w1)
		    	   if (!p.matcher(w).matches()) { return; }
		       
		       context.write(new Text(ngram), new LongWritable(Long.valueOf(count)));

		    
		}
	}

	public static class ReducerWordCountClass extends Reducer<Text,LongWritable,Text,LongWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
				Counter Counter = context.getCounter("my_counter", "totalwordApearing");
				Counter.increment(sum);
			}
			context.write(key, new LongWritable(sum)); 
		}
	}

	public static void main(String[] args) throws Exception {

	    Configuration conf = new Configuration();
		System.out.println("[~programer messege~] Staring step 2 .");

	    Job job = Job.getInstance(conf);
		job.setJarByClass(Step2.class);
		job.setMapperClass(MapperClassWordCounter1Gram.class);

		job.setReducerClass(ReducerWordCountClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("finished step2.");


	}

}
