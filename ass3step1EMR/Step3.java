import java.io.IOException;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3 {
	// input 1 gram
	// This step extract Hook Words & HighFrequency Words (HFW) and CW from word count
	// Parameters Fc, Fh, Fb


	//TODO add counter of max hookwords

	public static class myMapperClass extends Mapper<LongWritable, Text, Text, Text> {
		private static int Fc;					// Article: 100-5000 per miliion
		private static int Fh;			// Article: 10-100  per miliion
		private static int Fb;				// Article: 1-50  per miliion
		private static int maxHookWord;	// TODO Article: 100-1000 (N).    NEED TO CHECK WHY! Currently unused.


		private long occurences;
		private String classification;
		private String valueAsString;
		private String[] splittedValue;
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Fb = 50*100000;
			Fc = 1000*100000;
			Fc =  100*100000;
			maxHookWord = context.getConfiguration().getInt("maxHook", 2000);
		}

		
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

			classification = "";   // or hook or hfw or notCW
			valueAsString = value.toString();
			splittedValue = valueAsString.split("\t");
			occurences = Long.parseLong(splittedValue[1]);
			
			if (occurences < Fc && occurences > Fb) { // hook word // add only if we are below maxHookWords
				classification += "HOOK$";
				
				Counter Counter = context.getCounter("my_counter", "hooks");
				Counter.increment(1L);
			}

			if (occurences > Fc) { // most common words should return true here
				classification += "NOTCW$";
			}

			if (occurences > Fh) { // high frequency word
				classification += "HFW$";
//				if (Math.random() < 0.02) {
//					System.out.println("HFW: "+splittedValue[0]+"  Original: "+valueAsString);
//				}
			}
			// here splittedValue[0] is the word
			// classification example: Hook|HFW
			// classification example: Hook|
			// classification example: HFW|
			if(!classification.equals(""))
			context.write(new Text(splittedValue[0]), new Text(classification));
		}
	}

	public static class myReducerClass extends Reducer<Text,Text,Text,Text> {
		private static int maxHookWords;
		private MultipleOutputs<Text, Text> mos;
		private static long ratio;
		private static long counter = 0;
		private String[] wordClassifications;
		@Override
		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
			maxHookWords = context.getConfiguration().getInt("maxHook", 2000);

			Counter Counter = context.getCounter("my_counter", "hooks");
			System.out.println(Counter.getValue());
			//ratio = (long) Math.ceil((float) context.getCounter(COUNTER.HOOKWORDS).getValue() / (float) maxHookWords);
	
		}

	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {

			for (Text value : values) {
				wordClassifications = value.toString().split("\\$");
				for (int i=0 ; i<wordClassifications.length ; i++) {
					
					if (wordClassifications[i].equals("HOOK")) {
						// if we want multiple reducers, consider adding this context.getNumReduceTasks()) to the value of ratio
//						if ( (counter = (counter+1)%ratio) !=0)
//							continue;
						mos.write("hook", key, null);
					}
					else if (wordClassifications[i].equals("HFW"))
						mos.write("hfw", key, null);

					else if (wordClassifications[i].equals("NOTCW")) {
						mos.write("notcw", key, null);
					}
				}
			}

		}
		@Override
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
//		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);  
//        if(fileSystem.exists(outpath)){
//            fileSystem.delete(outpath, true);
//        }
	    
		conf.setInt("Fb", Integer.parseInt(args[2]));
		conf.setInt("Fc", Integer.parseInt(args[3]));
		conf.setInt("Fh", Integer.parseInt(args[4]));
		conf.setInt("maxHook", Integer.parseInt(args[5]));
		System.out.println("[~programer messege~] Staring step 3 .");
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step3.class);
		conf = job.getConfiguration();    //!!!!!!!!!!!!!!!!1 notice! have to get the updated conf

		job.setReducerClass(myReducerClass.class);
		job.setMapperClass(myMapperClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//TODO Change to the following lines when working with Lz0
		//		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "hook", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "hfw", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "notcw", TextOutputFormat.class,
				Text.class, Text.class);
		//job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
