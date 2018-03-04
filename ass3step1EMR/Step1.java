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
 

public class Step1 {

	public static class MapperClass5GramAgg extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private String valueAsString;
		private String[] splittedValue;
		private LongWritable occurences;
		Pattern p = Pattern.compile("[a-z]+");  // [a-zA-Z]+
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
	
		private static final int limiter = 5; 
		private static  int counter = 0;
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			if (  (counter= ((counter+1)%limiter) ) !=0 ) //skip every 4 = 20%
				return;        
			
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
