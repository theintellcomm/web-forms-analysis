package edu.poly.formsanalysis;

import org.apache.hadoop.io.IntWritable;

public class FormsAnalysisConfiguration {
	
	public static final String INPUT = "hdfs://hadoop01.poly.edu:8020/forms_analysis/input/domains_100k.tar_kvp.txt";
	
	public static final String OUTPUT = "hdfs://hadoop01.poly.edu:8020/forms_analysis/output";
	
	public static final String DATASET_STRING = "DEEPPEEP";
	
	public static final IntWritable ONE = new IntWritable(1);
	
	public static final String ALL_ELEMENTS = "ALL";

}