package edu.poly.formsanalysis.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

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

public class CheckBoxesCount {
	
	public static final String HADOOP_TASK_NAME = "CheckBoxesCount";

	
	public static class Map extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		
		private IntWritable numCheckBoxes = new IntWritable(0);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String rec = value.toString();
			String domain = rec.substring(0, rec.indexOf("::"));
			String url = rec.substring(rec.indexOf("::") + "::".length(), rec.indexOf("\t"));
			String formHTML = rec.substring(rec.indexOf("\t") + "\t".length());

			Integer _numCheckBoxes = 0;
			
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
			
			HTMLDocument.Iterator inputElementsIterator = doc.getIterator(HTML.Tag.INPUT);
			while(inputElementsIterator.isValid()) {
				String type = (String) inputElementsIterator.getAttributes().getAttribute(HTML.Attribute.TYPE);
				// If type "checkbox"
				if(type!=null && type.equalsIgnoreCase("checkbox")) {
					++_numCheckBoxes;
				}
				inputElementsIterator.next();
			}
			
			reader.close();
			
			numCheckBoxes.set(_numCheckBoxes);

			// Write per form count
			word.set(url);
			context.write(word, numCheckBoxes);

			// Write per domain count
			word.set(domain);
			context.write(word, numCheckBoxes);
			
			// Write count for entire dataset
			word.set(FormsAnalysisConfiguration.FORM_ELEMENTS_COUNT);
			context.write(word, numCheckBoxes);
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
		Configuration conf = new Configuration();
		Job job = new Job(conf, HADOOP_TASK_NAME);
		
		job.setJarByClass(CheckBoxesCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args.length>0 ? args[0] : FormsAnalysisConfiguration.INPUT));
		FileOutputFormat.setOutputPath(job, new Path(args.length>1 ? args[1] : FormsAnalysisConfiguration.OUTPUT + "/" + HADOOP_TASK_NAME));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}