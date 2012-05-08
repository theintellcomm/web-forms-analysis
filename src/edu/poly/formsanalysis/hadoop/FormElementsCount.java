package edu.poly.formsanalysis.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.swing.text.AttributeSet;
import javax.swing.text.Element;
import javax.swing.text.ElementIterator;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLDocument;
import javax.swing.text.html.HTMLEditorKit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.poly.formsanalysis.FormsAnalysisConfiguration;
import edu.poly.formsanalysis.hadoop.ElementNamesCount.Map;
import edu.poly.formsanalysis.hadoop.ElementNamesCount.Reduce;

public class FormElementsCount {

	public static final String HADOOP_TASK_NAME = "FormElementsCount";

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String rec = value.toString();
			String domain = rec.substring(0, rec.indexOf("::"));
			String url = rec.substring(rec.indexOf("::") + "::".length(),
					rec.indexOf("\t"));
			String formHTML = rec.substring(rec.indexOf("\t") + "\t".length());

			Hashtable<String, Integer> formElementCounts = new Hashtable<String, Integer>();
			formElementCounts.put(FormsAnalysisConfiguration.ALL_ELEMENTS, 0);

			HTMLEditorKit kit = new HTMLEditorKit();
			HTMLDocument doc = (HTMLDocument) kit.createDefaultDocument();
			Reader reader = new StringReader(formHTML);

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
					if (name == HTML.Tag.INPUT || name == HTML.Tag.SELECT
							|| name == HTML.Tag.TEXTAREA) {
						String type = null;
						if (name == HTML.Tag.INPUT) {
							type = (String) attributes
									.getAttribute(HTML.Attribute.TYPE);
							
							if(type==null 
								|| !(type.equalsIgnoreCase("BUTTON") || type.equalsIgnoreCase("CHECKBOX") 
									|| type.equalsIgnoreCase("FILE") || type.equalsIgnoreCase("HIDDEN")
									|| type.equalsIgnoreCase("PASSWORD") || type.equalsIgnoreCase("RADIO")
									|| type.equalsIgnoreCase("RESET") || type.equalsIgnoreCase("SUBMIT")
									|| type.equalsIgnoreCase("TEXT") || type.equalsIgnoreCase("IMAGE"))) {
								type = "OTHER";
							}
						} else if (name == HTML.Tag.SELECT) {
							type = "SELECT";
						} else if (name == HTML.Tag.TEXTAREA) {
							type = "TEXTAREA";
						}
						type = type.trim().toUpperCase();

						// Write total elements count for entire dataset
						word.set(FormsAnalysisConfiguration.DATASET_STRING
								+ "::"
								+ FormsAnalysisConfiguration.ALL_ELEMENTS);
						context.write(word, FormsAnalysisConfiguration.ONE);

						// Write count per each element type for entire dataset
						word.set(FormsAnalysisConfiguration.DATASET_STRING
								+ "::" + type);
						context.write(word, FormsAnalysisConfiguration.ONE);

						// Write total elements count for each domain
						word.set(domain + "::"
								+ FormsAnalysisConfiguration.ALL_ELEMENTS);
						context.write(word, FormsAnalysisConfiguration.ONE);

						// Write count per each element type for entire dataset
						word.set(domain + "::" + type);
						context.write(word, FormsAnalysisConfiguration.ONE);

						if (formElementCounts.containsKey(type)) {
							formElementCounts.put(type,
									formElementCounts.get(type) + 1);
						} else {
							formElementCounts.put(type, 1);
						}

						formElementCounts
								.put(FormsAnalysisConfiguration.ALL_ELEMENTS,
										formElementCounts
												.get(FormsAnalysisConfiguration.ALL_ELEMENTS) + 1);
					}
				}
			}

			reader.close();

			String tmp = null;
			Enumeration<String> keys = formElementCounts.keys();
			while (keys.hasMoreElements()) {
				String tmpKey = keys.nextElement();
				Integer _numTypeElements = formElementCounts.get(tmpKey);

				tmp = _numTypeElements.toString();
				if (_numTypeElements > 100) {
					int base = _numTypeElements / 100;
					tmp = (base * 100 + "-" + (base + 1) * 100);
				}

				// Write #forms in entire dataset having each element type count
				word.set(tmpKey + "::" + tmp);
				context.write(word, FormsAnalysisConfiguration.ONE);

				// Write #forms in each domain having each element type count
				word.set(tmpKey + "::" + domain + "::" + tmp);
				context.write(word, FormsAnalysisConfiguration.ONE);
			}

		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = FormsAnalysisConfiguration.INPUT;
		String output = FormsAnalysisConfiguration.OUTPUT;
		if(args.length==2) {
			input = args[0];
			output = args[1];
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, HADOOP_TASK_NAME);
		job.setJobName(HADOOP_TASK_NAME);

		job.setJarByClass(ElementNamesCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output + "/" + HADOOP_TASK_NAME));

		job.waitForCompletion(true);
	}
}
