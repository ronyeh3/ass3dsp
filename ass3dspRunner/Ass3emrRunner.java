

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;

public class Ass3emrRunner {

	private static final String HADOOP_VERSION = "2.7.3";
	private static final int INSTANCE_COUNT = 6;
	private static final String INSTANCE_TYPE = InstanceType.M3Xlarge.toString();


	private static final UUID RANDOM_UUID = UUID.randomUUID();
	private static final String FLOW_NAME = "ass3-" + RANDOM_UUID.toString();

	private static final String ONE_GRAM_CORPUS_3A = "s3a://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/data";
	private static final String FIVE_GRAM_CORPUS_3A = "s3a://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/5gram/data";

	private static final String JAR_BUCKET_NAME_S3 = "s3://jarsfromass3dsptamir";
	private static final String S3_STEP1_JAR = JAR_BUCKET_NAME_S3+"/step1.jar";
	private static final String S3_STEP2_JAR = JAR_BUCKET_NAME_S3 + "/step2.jar";
	private static final String S3_STEP3_JAR = JAR_BUCKET_NAME_S3 + "/step3.jar";
	private static final String S3_STEP6_JAR = JAR_BUCKET_NAME_S3 + "/step7.jar";


	private static final String STEP1_RESULT_FOLDER_NAME = "/step1Result/";
	private static final String STEP2_RESULT_FOLDER_NAME = "/step2Result/";
	private static final String STEP3_RESULT_FOLDER_NAME = "/step3Result/";
	private static final String STEP4_RESULT_FOLDER_NAME = "/step4Result/";
	private static final String STEP5_RESULT_FOLDER_NAME = "/step7Result/";

	//Individual buckets!!!
	private static final String REGION = "us-east-1d";
	private static final String S3N_LOG_URI = "s3n://aws-logs-489418265486-us-east-1/elasticmapreduce/";
	private static final String BUCKET_NAME = "ass3dsp181resultstamir";
	private static final String BUCKET_NAME_3A = "s3a://"+BUCKET_NAME; //old format
	private static final String BUCKET_NAME_3N = "s3n://"+BUCKET_NAME;   //new format
	//Ec2KeyName
	private static final String Ec2_Key_Name = "TamirDSP";

	public static void main(String[] args) {
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();

		AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)) //home\.aws\credentials
				.withRegion("us-east-1").build();

		Configuration configuration = new Configuration();
		configuration.addPropertiesEntry("mapreduce.reduce.java.opts", "-Xmx4096m");
		configuration.addPropertiesEntry("mapreduce.reduce.memory.mb", "8192");

		LinkedList<StepConfig> steps = new LinkedList<StepConfig>();
		/* ###############################################################################################*/

		//step1 preperation DONE
		//		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig( S3_STEP1_JAR);
		//		hadoopJarStep1.withArgs(FIVE_GRAM_CORPUS_3A,BUCKET_NAME_3N+ STEP1_RESULT_FOLDER_NAME);
		//
		//		
		//		StepConfig step1 = new StepConfig()
		//				.withName("Step1")
		//				.withHadoopJarStep(hadoopJarStep1)
		//				.withActionOnFailure("TERMINATE_JOB_FLOW");
		//		steps.add(step1);  


		//step2 preperation word count 		   
//		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig(S3_STEP2_JAR);
//		hadoopJarStep2.withArgs(ONE_GRAM_CORPUS_3A,BUCKET_NAME_3N+STEP2_RESULT_FOLDER_NAME);
//		//step2
//		StepConfig step2 = new StepConfig()
//				.withName("Step2")
//				.withHadoopJarStep(hadoopJarStep2)
//				.withActionOnFailure("TERMINATE_JOB_FLOW");			
//		steps.add(step2);



		
		//hadoop step3 // Find hookwords and hfws     
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig(S3_STEP3_JAR);
		hadoopJarStep3.withArgs(BUCKET_NAME_3N+STEP2_RESULT_FOLDER_NAME,BUCKET_NAME_3N+STEP3_RESULT_FOLDER_NAME);

		// 
		StepConfig step3 = new StepConfig()
				.withName("Step3")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");			
		steps.add(step3);

		//
		//		//hadoop step 4 		    
		//		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig( S3_STEP4_JAR);
		//		hadoopJarStep4.withArgs(BUCKET_NAME_3N+ STEP3_RESULT_FOLDER_NAME +"/",
		//				BUCKET_NAME_3N+ STEP2_RESULT_FOLDER_NAME ,
		//				BUCKET_NAME_3N+STEP4_RESULT_FOLDER_NAME);
		//
		//		//step4 
		//		StepConfig step4 = new StepConfig()
		//				.withName("Step4")
		//				.withHadoopJarStep(hadoopJarStep4)
		//				.withActionOnFailure("TERMINATE_JOB_FLOW");			
		//		steps.add(step4);
		//
		//


		//		//step6 		    
		//		HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig( S3_STEP6_JAR);
		//		hadoopJarStep6.withArgs(BUCKET_NAME_3N+ STEP5_RESULT_FOLDER_NAME ,BUCKET_NAME_3N+STEP6_RESULT_FOLDER_NAME);
		//
		//		//step6 config
		//		StepConfig step6 = new StepConfig()
		//				.withName("Step6")
		//				.withHadoopJarStep(hadoopJarStep6)
		//				.withActionOnFailure("TERMINATE_JOB_FLOW");			
		//		steps.add(step6);





		//the instance configuration
		JobFlowInstancesConfig instance = new JobFlowInstancesConfig();

		instance.setHadoopVersion(HADOOP_VERSION);
		instance.setInstanceCount(INSTANCE_COUNT);
		instance.setMasterInstanceType(INSTANCE_TYPE);
		instance.setSlaveInstanceType(INSTANCE_TYPE);
		instance.setKeepJobFlowAliveWhenNoSteps(false);
		instance.setEc2KeyName(Ec2_Key_Name);
		instance.withPlacement(new PlacementType(REGION));


		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest(FLOW_NAME, instance);
		runFlowRequest.setSteps(steps);
		runFlowRequest.setServiceRole("EMR_DefaultRole");
		runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
		runFlowRequest.setReleaseLabel("emr-5.11.0");
		runFlowRequest.setLogUri(S3N_LOG_URI);
		runFlowRequest.setVisibleToAllUsers(true);

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);


		try {
			waitForCompletion(jobFlowId , mapReduce);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.print("cluster "+mapReduce.listClusters().getClusters().get(0).getStatus().getState());
		System.out.println(": "+mapReduce.listClusters().getClusters().get(0).getStatus().getStateChangeReason().getCode());
		System.out.println("result are in https://s3.console.aws.amazon.com/s3/buckets/"+BUCKET_NAME);
	}

	static void waitForCompletion(String jobFlowId ,AmazonElasticMapReduce mapReduce) throws InterruptedException {
		int dotcount = 0;
		ListStepsResult steps = mapReduce.listSteps(new ListStepsRequest().withClusterId(jobFlowId));
		List<StepSummary> stepsList = steps.getSteps();
		for( int i=stepsList.size()-1; i>=0; i--) {
			String state = ""; //can be PENDING OR RUNUNIG
			StepSummary stepSummary = stepsList.get(i);
			System.out.print("starting " + stepSummary.getName());

			while(true) {	
				DescribeStepResult res = mapReduce.describeStep(new DescribeStepRequest().withStepId(stepsList.get(i).getId()).withClusterId(jobFlowId));
				String stepStatus = res.getStep().getStatus().getState().toUpperCase();

				if (stepStatus.equals("COMPLETED")) {
					System.out.println("\n"+stepSummary.getName() +" completed");
					break;//Continue with for
				}
				if (stepStatus.equals("FAILED") || stepStatus.equals("CANCELLED")) {
					System.err.println("\n"+stepSummary.getName() +" failed");
					return;
				}
				if ( ! stepStatus.equals(state)) {
					state = stepStatus;
					System.out.println("\n"+stepSummary.getName() + " is "+ state);
					dotcount = 0;
				}
				if ( (dotcount= ((dotcount+1)%100))==0)
					System.out.println(".");
				System.out.print(".");
				Thread.sleep(5000);

			}

		}
		Thread.sleep(5000);
	}
}
