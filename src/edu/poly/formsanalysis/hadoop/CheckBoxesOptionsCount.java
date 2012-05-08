package edu.poly.formsanalysis.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.swing.text.Element;
import javax.swing.text.ElementIterator;
import javax.swing.text.StyleConstants;
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

public class CheckBoxesOptionsCount {
	
	public static final String HADOOP_TASK_NAME = "CheckBoxOptionsCount";

	
	public static class Map extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String rec = value.toString();
			String domain = rec.substring(0, rec.indexOf("::"));
			String url = rec.substring(rec.indexOf("::") + "::".length(), rec.indexOf("\t"));
			String formHTML = rec.substring(rec.indexOf("\t") + "\t".length());

			HTMLEditorKit kit = new HTMLEditorKit();
			HTMLDocument doc = (HTMLDocument) kit.createDefaultDocument();
			Reader reader = new StringReader(formHTML);
			
			try {
				kit.read(reader, doc, 0);
			} catch (Exception e) {
				return;
			} catch(Throwable t) {
				return;
			}
			
			ElementIterator iterator = new ElementIterator(doc);
			Element element = null;
			
			Hashtable<String, Integer> checkboxesOptions = new Hashtable<String, Integer>();
			
			while((element=iterator.next())!=null) {
				Object tagName = element.getAttributes().getAttribute(StyleConstants.NameAttribute);
				if(tagName instanceof HTML.Tag && tagName==HTML.Tag.INPUT) {
					String type = (String) element.getAttributes().getAttribute(HTML.Attribute.TYPE);
					if(type!=null && type.equalsIgnoreCase("CHECKBOX")) {
						String nm = (String) element.getAttributes().getAttribute(HTML.Attribute.NAME);
						
						if (checkboxesOptions.containsKey(nm)) {
							checkboxesOptions.put(nm, checkboxesOptions.get(nm) + 1);
						} else {
							checkboxesOptions.put(nm, 1);
						}							
					}
				}
			}
			
			String tmp = null;
			Enumeration<String> keys = checkboxesOptions.keys();
			while (keys.hasMoreElements()) {
				String tmpKey = keys.nextElement();
				Integer _numTypeElements = checkboxesOptions.get(tmpKey);

				tmp = _numTypeElements.toString();
				if (_numTypeElements > 100) {
					int base = _numTypeElements / 100;
					tmp = (base * 100 + "-" + (base + 1) * 100);
				}
				
				word.set(FormsAnalysisConfiguration.DATASET_STRING + ":" + tmp);
				context.write(word, FormsAnalysisConfiguration.ONE);
				
				// Write #urls per each count
				word.set(domain + ":" + tmp);
				context.write(word, FormsAnalysisConfiguration.ONE);
			}
			
			reader.close();
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
