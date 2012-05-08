package edu.poly.formsanalysis.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

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
import edu.poly.formsanalysis.hadoop.ElementNamesCount.Map;
import edu.poly.formsanalysis.hadoop.ElementNamesCount.Reduce;

public class ListBoxOptionsCount {
	
	public static final String HADOOP_TASK_NAME = "ListBoxOptionsCount";

	
	public static class Map extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String rec = value.toString();
			String domain = rec.substring(0, rec.indexOf("::"));
			String url = rec.substring(rec.indexOf("::") + "::".length(), rec.indexOf("\t"));
			String formHTML = rec.substring(rec.indexOf("\t") + "\t".length());
			
			formHTML = formHTML.toLowerCase();
			int s = formHTML.indexOf("<select");
			int e = -1;
			do {
				e = formHTML.indexOf("</select>", s);
				if(e!=-1) {
					String tmp = formHTML.substring(s, e);
					int lastIndex = 0;
					int count = 0;
					while ((lastIndex = tmp.indexOf("<option", lastIndex)) != -1) {
						count++;
						lastIndex += 7;
					}
					
					String k = "" + count;
					if (count > 100) {
						int base = count / 100;
						k = (base * 100 + "-" + (base + 1) * 100);
					}

					// Write #in entire dataset 
					word.set(FormsAnalysisConfiguration.DATASET_STRING + "::" + k);
					context.write(word, FormsAnalysisConfiguration.ONE);

					// Write # in each domain
					word.set(domain + "::" + k);
					context.write(word, FormsAnalysisConfiguration.ONE);
					
					s = formHTML.indexOf("<select", e);
				}
			} while(s!=-1 && e!=-1);
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
