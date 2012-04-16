package edu.poly.formsanalysis;

import java.io.File;

/**
 * @author bhaktavatsalam
 *
 */
public class FormsAnalysisMain {
	
	public static void main(String args[]) throws Exception {
		if(args.length==1) {
			long t = System.currentTimeMillis();
			TarToTextFile.execute(new File(args[0]));
			System.out.println((System.currentTimeMillis() - t) + "ms; " 
					+ TarToTextFile.numSuccess + "/" + TarToTextFile.numTotal);
		} else {
			System.out.println("=========================================================================");
			System.out.println("Forms Analysis (by) Bhakta, Joseph and May, Spring 2012");
			System.out.println("=========================================================================");
			System.out.println("1. Convert deeppeep tar file into hadoop ready key value pairs");
			System.out.println("file. Each form file in the dataset is a record in the file.");
			System.out.println("Single Record in kvp file: DOMAIN::URL\\tFORM-HTML\\n\n");
			System.out.println("  java -cp ./lib/ant.jar:./lib/commons-cli-1.2.jar:./FormsAnalysis.jar ");
			System.out.println("	edu.poly.formsanalysis.FormsAnalysisMain <LOCATION-OF-DEEPPEEP-TAR>\n");
			System.out.println("A .txt file with the same name as the source is created.");
			System.out.println("Failed files will be printed to ERR stream.\n");
			System.out.println("2. HADOOP tasks:");
			System.out.println("Each hadoop task can be run using the following command:\n");
			System.out.println("  hadoop jar ./FormsAnalysis.jar <HADOOP-TASK> [<INPUT-FILE> <OUTPUT-FILE>]\n");
			System.out.println("By default \"/input/domains_100k.tar_kvp.txt\" and \"/output/<TASK-NAME>\" on");
			System.out.println("hdfs://hadoop01.poly.edu:8020/forms_analysis/ are automatically used, but you");
			System.out.println("can specify different ones for testing purposes. Here is a list of all the");
			System.out.println("hadoop tasks available:\n");
			System.out.println("  WordCount: edu.poly.formsanalysis.hadoop.WordCount");
			System.out.println("  EntriesPerDomain: edu.poly.formsanalysis.hadoop.EntriesPerDomain");
			System.out.println("\n=========================================================================");
		}
	}

}
