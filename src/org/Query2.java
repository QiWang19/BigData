package org;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Query2 {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		public void map (LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter repoter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			String[] strs = new String[tokenizer.countTokens()];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				strs[i] = tokenizer.nextToken().toString();
				i = i + 1;
			}
			word.set(strs[1]);	//CustId
			String outPut = strs[2];	// TransTotal
			Text t = new Text();
			t.set(outPut);
			output.collect(word, t);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			//super.reduce(key, values, output, reporter);
			long count = 0;
			double sum = 0;
			while (values.hasNext()) {
				String s = values.next().toString();
				Double t = Double.parseDouble(s);
				sum = sum  + t;
				count = count + 1;
			}
			
			String l1 = Double.toString(sum);
			String l2 = Long.toString(count);
			Text res = new Text();
			res.set(l1 + "    " + l2);
			output.collect(key, res);
		}
		
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(Query2.class);
		conf.setJobName("query2");
		
		// type of output <k,v> of reduce
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//job use mapper and reducer
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		//store path raw data
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//result path
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
	}

}

