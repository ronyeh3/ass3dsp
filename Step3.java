import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3 {

	// This step extract Hook Words & HighFrequency Words (HFW)
	// Parameters Fc, Fh, Fb
	public static class MapperClassWordCounter5Gram extends Mapper<LongWritable, Text, Text, Text> {
		private static final int Fc = 2500;				// Article: 100-5000
		private static final int Fh = 50;				// Article: 10-100
		private static final int Fb = 25;				// Article: 1-50
		private static final int maxHookWords = 1000;	// TODO Article: 100-1000 (N).    NEED TO CHECK WHY! Currently unused.
		private int occurences;
		private String classification;
		private String valueAsString;
		private String[] splittedValue;
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			classification = "";
			valueAsString = value.toString();
			splittedValue = valueAsString.split("\t");
			occurences = Integer.parseInt(splittedValue[1]);
			if (occurences < Fc && occurences > Fb) { // hook word
				classification += "Hook|";
			}
			if (occurences > Fh) { // high frequency word
				classification += "HFW|";
			}
			context.write(new Text(splittedValue[0]), new Text(classification));
		}
	}

	public static class ReducerWordCountClass extends Reducer<Text,Text,Text,Text> {
		private MultipleOutputs<Text,Text> mos;
		public void setup(Context context) {
			mos = new MultipleOutputs<Text,Text>(context);
		}
		private String[] wordClassifications;
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			for (Text value : values) {
				wordClassifications = value.toString().split("\\|");
				for (int i=0 ; i<wordClassifications.length ; i++) {
					if (wordClassifications[i].equals("Hook"))
						mos.write("hook", key, null);
					else if (wordClassifications[i].equals("HFW"))
						mos.write("hfw", key, null);
				}
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
		Configuration conf = new Configuration();
		Job job = new Job(conf, "HFW & Hook Words Counter");
		job.setJarByClass(Step1.class);
		job.setCombinerClass(ReducerWordCountClass.class);
		job.setReducerClass(ReducerWordCountClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapperClassWordCounter5Gram.class);
		//TODO Change to the following lines when working with Lz0
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//MultipleInputs.addInputPath(job,new Path(args[0]),SequenceFileInputFormat.class,MapperClass5Gram.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "hook", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "hfw", TextOutputFormat.class,
				Text.class, Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
