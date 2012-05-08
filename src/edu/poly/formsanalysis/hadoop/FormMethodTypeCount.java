package edu.poly.formsanalysis.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

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

public class FormMethodTypeCount {

	public static final String HADOOP_TASK_NAME = "FormMethodTypeCount";

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String rec = value.toString();
			String domain = rec.substring(0, rec.indexOf("::"));
			String url = rec.substring(rec.indexOf("::") + "::".length(),
					rec.indexOf("\t"));
			String formHTML = rec.substring(rec.indexOf("\t") + "\t".length());

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
					if (name == HTML.Tag.FORM) {
						String method = (String) attributes
								.getAttribute(HTML.Attribute.METHOD);
						
						if(method==null || !(method.equalsIgnoreCase("GET") 
								|| method.equalsIgnoreCase("POST") || method.equalsIgnoreCase("PUT") 
								|| method.equalsIgnoreCase("DELETE"))) {
							method = "OTHER";
						}
						method = method.toUpperCase();
						
						word.set(method);
						context.write(word, FormsAnalysisConfiguration.ONE);

						word.set(domain + "::" + method);
						context.write(word, FormsAnalysisConfiguration.ONE);
					}
				}
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
