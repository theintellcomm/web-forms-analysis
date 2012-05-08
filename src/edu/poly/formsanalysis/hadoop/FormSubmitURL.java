package edu.poly.formsanalysis.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;

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

public class FormSubmitURL {
	
	public static final String HADOOP_TASK_NAME = "FormSubmitURL";

	
	public static class Map extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		
		private IntWritable numListBoxOptions = new IntWritable(0);
		
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
			} catch (Throwable t) {
				return;
			}
			
			ElementIterator iterator = new ElementIterator(doc);
			Element element = null;
			while((element=iterator.next())!=null) {
				Object tagName = element.getAttributes().getAttribute(StyleConstants.NameAttribute);
				if(tagName instanceof HTML.Tag && tagName==HTML.Tag.FORM) {
					String action = (String) element.getAttributes().getAttribute(HTML.Attribute.ACTION);
					if(action!=null) {
						boolean sameDomain = false;
						
						URL origURL = null;
						try {
							origURL = new URL(url);
							origURL.toURI();
						} catch(Exception e) {
						}
					
						URL u = null;
						try {
							u = new URL(action);
							u.toURI();
							sameDomain = url.contains(u.getHost());
						} catch(Exception e) {
							sameDomain = true;
							u = null;
						}

						String protocol = null;
						if(sameDomain) {
							word.set("SAME_URL");
							context.write(word, FormsAnalysisConfiguration.ONE);
							
							// Write #urls per each count
							word.set(domain + "::SAME_URL");
							context.write(word, FormsAnalysisConfiguration.ONE);
					
							if(u!=null) {
								protocol = u.getProtocol();
							} else if(origURL!=null) {
								protocol = origURL.getProtocol();
							}
						} else {
							word.set("DIFFERENT_URL");
							context.write(word, FormsAnalysisConfiguration.ONE);
							
							// Write #urls per each count
							word.set(domain + "::DIFFERENT_URL");
							context.write(word, FormsAnalysisConfiguration.ONE);
					
							if(u!=null) {
								protocol = u.getProtocol();
							}
						}	
					
						if(protocol!=null) {
							if(protocol.equalsIgnoreCase("https")) {
								word.set("HTTPS");
								context.write(word, FormsAnalysisConfiguration.ONE);
								
								// Write #urls per each count
								word.set(domain + "::HTTPS");
								context.write(word, FormsAnalysisConfiguration.ONE);
							} else if(protocol.equalsIgnoreCase("http")) {
								word.set("HTTP");
								context.write(word, FormsAnalysisConfiguration.ONE);
								
								// Write #urls per each count
								word.set(domain + "::HTTP");
								context.write(word, FormsAnalysisConfiguration.ONE);
							}
						}
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
