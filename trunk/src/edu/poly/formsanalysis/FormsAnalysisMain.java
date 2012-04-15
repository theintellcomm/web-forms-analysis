package edu.poly.formsanalysis;

import java.io.File;

/**
 * @author bhaktavatsalam
 *
 */
public class FormsAnalysisMain {
	
	public static void main(String args[]) throws Exception {
		if(args.length==0) {
			System.out.println("Forms Analysis (by) Bhakta, Joseph and May, Spring 2012");
			System.out.println("==================================================================");
			System.out.println("Convert deeppeep tar file into hadoop ready key value pairs");
			System.out.println("file. Each form file in the dataset is a record in the file.");
			System.out.println("Single Record in kvp file: DOMAIN::URL\\tFORM-HTML\\n\n");
			System.out.println("  java -jar <LOCATION-OF-THIS-JAR> <LOCATION-OF-DEEPPEEP-TAR>\n");
			System.out.println("HADOOP tasks:\n--------------");
			System.out.println("1. Form Elements count: edu.poly.formsanalysis.hadoop.FormElementsCount");
		} else {
			TarToTextFile.execute(new File(args[0]));
		}
	}

}
