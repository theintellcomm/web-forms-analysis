package edu.poly.formsanalysis;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;

import javax.swing.text.Element;
import javax.swing.text.ElementIterator;
import javax.swing.text.StyleConstants;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLDocument;
import javax.swing.text.html.HTMLEditorKit;

import edu.poly.formsanalysis.hadoop.CheckBoxesOptionsCount;
import edu.poly.formsanalysis.hadoop.ElementNamesCount;
import edu.poly.formsanalysis.hadoop.EntriesPerDomain;
import edu.poly.formsanalysis.hadoop.FormByteSize;
import edu.poly.formsanalysis.hadoop.FormElementsCount;
import edu.poly.formsanalysis.hadoop.FormMethodTypeCount;
import edu.poly.formsanalysis.hadoop.FormSubmitURL;
import edu.poly.formsanalysis.hadoop.ListBoxOptionsCount;
import edu.poly.formsanalysis.hadoop.RadioButtonOptionsCount;
import edu.poly.formsanalysis.hadoop.StructuralAnalysis;

/**
 * @author bhaktavatsalam
 * 
 */
public class FormsAnalysisMain {

	public static String sampleFormHTML = "<html><body><form action='http://now.com' method='post'><input type='radio' name='a' value='1' /><input type='radio' name='a' value='2' /><input type='text' name='asd' /><select name='aaa'><OPTION name='one'>vs</OPTION><option>as</option></select><select></select></form></body></html>";

	public static void main(String args[]) throws Exception {
			if (args.length==1 && args[0].equalsIgnoreCase("help")) {
				System.out.println("=========================================================================");
				System.out.println("Forms Analysis (by) Bhakta, Joseph and May, Spring 2012");
				System.out.println("=========================================================================");
				System.out.println("1. Convert deeppeep tar file into hadoop ready key value pairs");
				System.out.println("file. Each form file in the dataset is a record in the file.");
				System.out.println("Single Record in kvp file: DOMAIN::URL\\tFORM-HTML\\n\n");
				System.out.println("  java -cp <PATH>/ant.jar:<PATH>/commons-cli-1.2.jar:./FormsAnalysis.jar ");
				System.out.println("	edu.poly.formsanalysis.FormsAnalysisMain <LOCATION-OF-DEEPPEEP-TAR>\n");
				System.out.println("A .txt file with the same name as the source is created.");
				System.out.println("Failed files will be printed to ERR stream.\n");
				System.out.println("2. HADOOP tasks:");
				System.out.println("All hadoop tasks can be run using the following command:");
				System.out.println("  hadoop jar ./FormsAnalysis.jar edu.poly.formsanalysis.FormsAnalysisMain [<INPUT-FILE> <OUTPUT-DIR>]\n");
				System.out.println("To run individual tasks, use the following command:");
				System.out.println("  hadoop jar ./FormsAnalysis.jar <HADOOP-TASK> [<INPUT-FILE> <OUTPUT-DIR>]\n");
				System.out.println("Default <INPUT-FILE>: hdfs://hadoop01.poly.edu:8020/forms_analysis/input/domains_100k.tar_kvp.txt");
				System.out.println("Default <OUTPUT-DIR>: hdfs://hadoop01.poly.edu:8020/forms_analysis/output/<TASK-NAME>\n");
				System.out.println("  EntriesPerDomain: edu.poly.formsanalysis.hadoop.EntriesPerDomain");
				System.out.println("  FormMethodTypeCount: edu.poly.formsanalysis.hadoop.FormMethodTypeCount");
				System.out.println("  FormElementsCount: edu.poly.formsanalysis.hadoop.FormElementsCount");
				System.out.println("  ElementNamesCount: edu.poly.formsanalysis.hadoop.ElementNamesCount");
				System.out.println("  FormByteSize: edu.poly.formsanalysis.hadoop.FormByteSize");
				System.out.println("  ListBoxOptionsCount: edu.poly.formsanalysis.hadoop.ListBoxOptionsCount");
				System.out.println("  CheckBoxesOptionsCount: edu.poly.formsanalysis.hadoop.CheckBoxesOptionsCount");
				System.out.println("  RadioButtonOptionsCount: edu.poly.formsanalysis.hadoop.RadioButtonOptionsCount");
				System.out.println("  PermutationsCount: edu.poly.formsanalysis.hadoop.PermutationsCount");
				System.out.println("  StructuralAnalysis: edu.poly.formsanalysis.hadoop.StructuralAnalysis");
				System.out.println("\n=========================================================================");
			} else if(args.length==1) {
				long t = System.currentTimeMillis();
				TarToTextFile.execute(new File(args[0]));
				System.out.println((System.currentTimeMillis() - t) + "ms; "
						+ TarToTextFile.numSuccess + "/"
						+ TarToTextFile.numTotal);
			} else if (args.length==0 || args.length==2) {
				EntriesPerDomain.main(args);
				FormMethodTypeCount.main(args);
				FormElementsCount.main(args);
				ElementNamesCount.main(args);
				StructuralAnalysis.main(args);
				FormByteSize.main(args);
				ListBoxOptionsCount.main(args);
				CheckBoxesOptionsCount.main(args);
				RadioButtonOptionsCount.main(args);
				FormSubmitURL.main(args);
			}
	}

}
