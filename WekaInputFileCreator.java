import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;


/* 
 * 
 * input - args[0] - Step7 folder (cluster semantic relation vectors)
 * output - semantic-relations.arff
 * 
 */

public class WekaInputFileCreator {

	public static void main(String[] args) throws Exception {
		BufferedWriter out = new BufferedWriter(new FileWriter("output/weka/semantic_relations.arff"));
		BufferedWriter debug = new BufferedWriter(new FileWriter("output/weka/semantic_debug.arff"));

		out.write(wekaHeader());
		out.write(wekaAttributes(args[0]));
		
		debug.write(wekaHeader());
		debug.write(wekaAttributes(args[0]));
		
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith("part-r-"); // read the step7 reducer output files
			}};
			File folder = new File(args[0]);
			File[] listOfFiles = folder.listFiles(filter);
			String strLine;
			String[] vectorAndNgrams;

			out.write("@DATA\r\n");
			debug.write("@DATA\r\n");
			for (int i = 0; i < listOfFiles.length; i++) {
				File file = listOfFiles[i];

				FileInputStream fstream = new FileInputStream(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
				while ((strLine = br.readLine()) != null) { 
					vectorAndNgrams = strLine.split("##");
					out.write(vectorAndNgrams[0]+"\r\n");
					debug.write("% "+vectorAndNgrams[1]+"\r\n"+vectorAndNgrams[0]+"\r\n\r\n");
				}
				br.close();
			}
			out.close();
			debug.close();
	}
	
	
	

	private static String wekaHeader() {
		return
				"   % 1. Title: DSP181 - Final Project\r\n" + 
				"   % \r\n" + 
				"   % 2. Sources:\r\n" + 
				"   %      (a) Creators: Ron Yehuda & Tamir Eyal\r\n" + 
				"   %      (b) Date: March, 2018\r\n" + 
				"   % \r\n\r\n" + 
				"  @RELATION semanticsRelations\r\n\r\n";
	}

	
	
	private static String wekaAttributes(String pathToVectors) throws IOException {
		File file = new File(pathToVectors+"/part-r-00000");
		FileInputStream fstream = new FileInputStream(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String[] firstVectorAndNgrams = br.readLine().split("##");
		String firstVector = firstVectorAndNgrams[0];
		String[] vectorData = firstVector.split(",");
		String attributes = "";
		for (int i=0 ; i<vectorData.length-1; i++) {
			attributes += "  @ATTRIBUTE cluster"+i+"   NUMERIC\r\n";
		}
		attributes += "  @ATTRIBUTE class        {coord,hyper,mero,random}\r\n\r\n";
		br.close();
		return attributes;
	}

}

