package edu.poly.formsanalysis;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;

import javax.swing.text.AttributeSet;
import javax.swing.text.Element;
import javax.swing.text.ElementIterator;
import javax.swing.text.StyleConstants;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLDocument;
import javax.swing.text.html.HTMLEditorKit;

/**
 * @author bhaktavatsalam
 * 
 */
public class FormsAnalysisMain {

	public static String sampleFormHTML = "<html><form method='post'></form><input type='text' /><select><option></option></select></html>";

	public static void main(String args[]) throws Exception {
		if (args.length == 1) {
			if (args[0].equalsIgnoreCase("help")) {
				System.out
						.println("=========================================================================");
				System.out
						.println("Forms Analysis (by) Bhakta, Joseph and May, Spring 2012");
				System.out
						.println("=========================================================================");
				System.out
						.println("1. Convert deeppeep tar file into hadoop ready key value pairs");
				System.out
						.println("file. Each form file in the dataset is a record in the file.");
				System.out
						.println("Single Record in kvp file: DOMAIN::URL\\tFORM-HTML\\n\n");
				System.out
						.println("  java -cp ./lib/ant.jar:./lib/commons-cli-1.2.jar:./FormsAnalysis.jar ");
				System.out
						.println("	edu.poly.formsanalysis.FormsAnalysisMain <LOCATION-OF-DEEPPEEP-TAR>\n");
				System.out
						.println("A .txt file with the same name as the source is created.");
				System.out
						.println("Failed files will be printed to ERR stream.\n");
				System.out.println("2. HADOOP tasks:");
				System.out
						.println("Each hadoop task can be run using the following command:\n");
				System.out
						.println("  hadoop jar ./FormsAnalysis.jar <HADOOP-TASK> [<INPUT-FILE> <OUTPUT-FILE>]\n");
				System.out
						.println("By default \"/input/domains_100k.tar_kvp.txt\" and \"/output/<TASK-NAME>\" on");
				System.out
						.println("hdfs://hadoop01.poly.edu:8020/forms_analysis/ are automatically used, but you");
				System.out
						.println("can specify different ones for testing purposes. Here is a list of all the");
				System.out.println("hadoop tasks available:\n");
				System.out
						.println("  EntriesPerDomain: edu.poly.formsanalysis.hadoop.EntriesPerDomain");
				System.out
						.println("  FormElementsCount: edu.poly.formsanalysis.hadoop.FormElementsCount");
				System.out
						.println("  TextBoxesCount: edu.poly.formsanalysis.hadoop.TextBoxesCount");
				System.out
						.println("  PasswordBoxesCount: edu.poly.formsanalysis.hadoop.PasswordBoxesCount");
				System.out
						.println("  TextAreasCount: edu.poly.formsanalysis.hadoop.TextAreasCount");
				System.out
						.println("  ListBoxesCount: edu.poly.formsanalysis.hadoop.ListBoxesCount");
				System.out
						.println("  CheckBoxesCount: edu.poly.formsanalysis.hadoop.CheckBoxesCount");
				System.out
						.println("  RadioButtonsCount: edu.poly.formsanalysis.hadoop.RadioButtonsCount");
				System.out
						.println("  HiddenElementsCount: edu.poly.formsanalysis.hadoop.HiddenElementsCount");
				System.out
						.println("  RegularButtonsCount: edu.poly.formsanalysis.hadoop.RegularButtonsCount");
				System.out
						.println("  SubmitButtonsCount: edu.poly.formsanalysis.hadoop.SubmitButtonsCount");
				System.out
						.println("  ResetButtonsCount: edu.poly.formsanalysis.hadoop.ResetButtonsCount");
				System.out
						.println("  FormMethodTypeCount: edu.poly.formsanalysis.hadoop.FormMethodTypeCount");
				System.out.println("\n");
				System.out
						.println("  ListBoxOptionsCount: edu.poly.formsanalysis.hadoop.ListBoxOptionsCount");
				System.out
						.println("\n=========================================================================");
			} else {
				long t = System.currentTimeMillis();
				TarToTextFile.execute(new File(args[0]));
				System.out.println((System.currentTimeMillis() - t) + "ms; "
						+ TarToTextFile.numSuccess + "/"
						+ TarToTextFile.numTotal);
			}
		} else {
			HTMLEditorKit kit = new HTMLEditorKit();
			HTMLDocument doc = (HTMLDocument) kit.createDefaultDocument();
			Reader reader = new StringReader(sampleFormHTML);

			try {
				kit.read(reader, doc, 0);
			} catch (Exception e) {
				return;
			} catch (Throwable t) {
				return;
			}

			ElementIterator iterator = new ElementIterator(doc);
			Element el = null;
			while ((el = iterator.next()) != null) {
				AttributeSet attributes = el.getAttributes();
				if (attributes != null) {
					Object name = attributes
							.getAttribute(AttributeSet.NameAttribute);
					if (name == HTML.Tag.INPUT) {
						String type = (String) attributes.getAttribute(HTML.Attribute.TYPE);
						// If type is empty or "text"
						if(type!=null && (type.isEmpty() || type.equalsIgnoreCase("text"))) {
							System.out.println(type);
						}
						
						System.out.println("AAA");
					} else if(name == HTML.Tag.SELECT
							|| name == HTML.Tag.TEXTAREA) {
						System.out.println(name);
					}
				}
			}

			reader.close();

			// TextBoxesCount.main(args);
			// PasswordBoxesCount.main(args);
			// TextAreasCount.main(args);
			//
			// ListBoxesCount.main(args);
			// CheckBoxesCount.main(args);
			// RadioButtonsCount.main(args);
			//
			// HiddenElementsCount.main(args);
			// FileElementsCount.main(args);
			//
			// RegularButtonsCount.main(args);
			// ResetButtonsCount.main(args);
			// SubmitButtonsCount.main(args);
			//

			// ElementNamesCount.main(args);
			// ListBoxOptionsCount.main(args);
			// CheckBoxesOptionsCount.main(args);

			// // CORRECTED:
			// FormMethodTypeCount.main(args);
			//
			// EntriesPerDomain.main(args);
			//
			// FormElementsCount.main(args);

		}
	}

}
