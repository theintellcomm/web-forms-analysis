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

public class FormMethodTypeCount {
	
	public static final String HADOOP_TASK_NAME = "FormMethodTypeCount";

	
	public static class Map extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		
		private IntWritable numGet = new IntWritable(0);
		
		private IntWritable numPost = new IntWritable(0);
		
		private IntWritable numPut = new IntWritable(0);

		private IntWritable numHead = new IntWritable(0);
		
		private IntWritable numDelete = new IntWritable(0);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String rec = value.toString();
			String domain = rec.substring(0, rec.indexOf("::"));
			String url = rec.substring(rec.indexOf("::") + "::".length(), rec.indexOf("\t"));
			String formHTML = rec.substring(rec.indexOf("\t") + "\t".length());

			Integer _numGet = 0;
			Integer _numPost = 0;
			Integer _numPut = 0;
			Integer _numHead = 0;
			Integer _numDelete = 0;
			
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
			
			HTMLDocument.Iterator inputElementsIterator = doc.getIterator(HTML.Tag.FORM);
			while(inputElementsIterator.isValid()) {
				String method = (String) inputElementsIterator.getAttributes().getAttribute(HTML.Attribute.METHOD);
				if(method!=null) {
					if(method.equalsIgnoreCase("post")) {
						++_numPost;
					} else if(method.equalsIgnoreCase("put")) {
						++_numPut;
					} else if(method.equalsIgnoreCase("head")) {
						++_numHead;
					} else if(method.equalsIgnoreCase("delete")) {
						++_numDelete;
					} else {
						++_numGet;
					}
				} else {
					++_numGet;
				}
				inputElementsIterator.next();
			}
			
			reader.close();
			
			numGet.set(_numGet);

			word.set("GET");
			context.write(word, numGet);

			word.set("POST");
			context.write(word, numPost);

			word.set("PUT");
			context.write(word, numPut);

			word.set("DELETE");
			context.write(word, numDelete);

			word.set("HEAD");
			context.write(word, numHead);

			// Write per domain count
			word.set(domain + ":GET");
			context.write(word, numGet);

			word.set(domain + ":POST");
			context.write(word, numPost);

			word.set(domain + ":PUT");
			context.write(word, numPut);

			word.set(domain + ":DELETE");
			context.write(word, numDelete);

			word.set(domain + ":HEAD");
			context.write(word, numHead);
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
		job.setJobName(HADOOP_TASK_NAME);
		
		job.setJarByClass(FormMethodTypeCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(FormsAnalysisConfiguration.INPUT));
		FileOutputFormat.setOutputPath(job, new Path(FormsAnalysisConfiguration.OUTPUT + "/" + HADOOP_TASK_NAME));
		
		job.waitForCompletion(true);
	}
}
