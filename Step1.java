import java.io.File;
//comment
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class Step1 {
	
	// This step aggregates identical 5Grams from different years

	public static class MapperClass5GramAgg extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private String valueAsString;
		private String[] splittedValue;
		private LongWritable occurences;
		Pattern p = Pattern.compile("[a-zA-Z]+");
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			valueAsString = value.toString();
			splittedValue = valueAsString.split("\t");
			String[] splittedNgram = splittedValue[0].split("\\s+");
			for (String word : splittedNgram)
				if (!p.matcher(word).matches()) { return; }
			occurences = new LongWritable(Long.parseLong(splittedValue[2]));
			word.set(splittedValue[0]);  
			context.write(word, occurences);
		}
	}

	public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			long sum = 0;
			for (LongWritable value : values)
				sum += value.get();			
			context.write(key, new LongWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
//		System.load("C:/Users/Tamir/Desktop/lzo2.dll");
//		System.setProperty("hadoop.home.dir", "C:/hadoop-2.6.2");
		
		System.load("C:/Users/RONlptp/eclipse-workspace/ass2localRunner/lib/lzo2.dll");
		System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.2");
		Configuration conf = new Configuration();
		Job job = new Job(conf, "5Grams Aggregator");
		job.setJarByClass(Step1.class);
		job.setCombinerClass(ReducerClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapperClass5GramAgg.class);
		//TODO Change to the following lines when working with Lz0
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//MultipleInputs.addInputPath(job,new Path(args[0]),SequenceFileInputFormat.class,MapperClass5Gram.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
	
}

