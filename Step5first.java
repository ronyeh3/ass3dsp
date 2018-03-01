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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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

public class Step5first {
	
	// This step aggregates identical 5Grams from different years

	public static class MapperClass5GramAgg extends Mapper<LongWritable, Text, Pairkey, Text> {
		private String valueAsString;
		private String[] splittedValue;
		private String Hook;
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			valueAsString = value.toString();
			splittedValue = valueAsString.split("\t");
			Hook = splittedValue[0];
			String[] targetsAndPattens = splittedValue[1].split("\\|");
			
			for (String TandG : targetsAndPattens) {

		         	String[] separateTandG = TandG.split("##");
		         	Pairkey taggedKey = new Pairkey(Hook , separateTandG[1]);
					context.write(taggedKey, new Text(separateTandG[0]));
			}
			
		}
	}

	public static class ReducerClass extends Reducer<Pairkey,Text,Text,Text> {
		@Override
		public void reduce(Pairkey key , Iterable<Text> values, Context context) throws IOException,  InterruptedException {

			String[] hookandT = key.split("**");
			
			
			splittedPatternAndTarget = patternAndTarget.split("##");
			for (LongWritable value : values)
				sum += value.get();			
			context.write(key, new LongWritable(sum));
		}
	}
	
	
	
	public static class PairkeyGroupingComparator extends WritableComparator{

		protected PairkeyGroupingComparator() {
			super(Pairkey.class,true);
		}

		@Override
		public int compare(WritableComparable o1,WritableComparable o2){
			Pairkey p1 = (Pairkey) o1;
			Pairkey p2 = (Pairkey) o2;
			int cmp = p1.getrealkey().compareTo(p2.getrealkey());
			return cmp;

		}

	}

	public static class pairkeySortingComparator extends WritableComparator{

		protected pairkeySortingComparator() {
			super(Pairkey.class,true);
		}

		@Override
		public int compare(WritableComparable o1,WritableComparable o2){
			Pairkey p1 = (Pairkey) o1;
			Pairkey p2 = (Pairkey) o2;
			int cmp = p1.getrealkey().compareTo(p2.getrealkey());
			if(cmp == 0){
				cmp = p1.getFname().compareTo(p2.getFname());
			}
			return cmp;
		}

	}

	public static class pairkeyPartitioner extends Partitioner<Pairkey, Text>{

		@Override
		public int getPartition(Pairkey key, Text value, int nuOfReducers) {
			
			return Math.abs(key.getrealkey().hashCode()) % nuOfReducers;
		}
	}
	
	
	public static void main(String[] args) throws Exception {
//		System.load("C:/Users/Tamir/Desktop/lzo2.dll");
//		System.setProperty("hadoop.home.dir", "C:/hadoop-2.6.2");
		
		System.load("C:/Users/RONlptp/eclipse-workspace/ass2localRunner/lib/lzo2.dll");
		System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.2");
		Configuration conf = new Configuration();
		Job job = new Job(conf, "first Aggregator");
		job.setJarByClass(Step5first.class);
		job.setCombinerClass(ReducerClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path hfwAndHooks = new Path(args[0]+"[byHook]*");  //!!!!!!!!!! input
		MultipleInputs.addInputPath(job,hfwAndHooks,TextInputFormat.class,MapperClass5GramAgg.class);
		//TODO Change to the following lines when working with Lz0
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//MultipleInputs.addInputPath(job,new Path(args[0]),SequenceFileInputFormat.class,MapperClass5Gram.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
	
}

