package org;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Query4 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new Configuration(),
						new Query4(), args);

	}
	
	
	
	public static class Map extends  Mapper<LongWritable, Text, Text, Text> {
		private HashMap<String ,String> h = new HashMap<String, String>();
		private BufferedReader br;
		public void setup(Context context) throws IOException {
			Path[]  paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path: paths) {
				if (path.getName().toString().trim().equals("customers")) {
					br = new BufferedReader(new FileReader(path.toString()));
					String line = br.readLine();
					while (line != null) {
						String[] splits = line.split(",");
						h.put(splits[0], splits[3]);
						line = br.readLine();
					}
				}
			}
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			String countryCode = h.get(strs[1]);
			Double TransTotal = Double.parseDouble(strs[2]);
			
			if (countryCode != null) {
				Text k = new Text();
				Text v = new Text();
				k.set(countryCode);
				v.set("1," + TransTotal);
				context.write(k, v);
			}
		}
		
	}
	
	public static class Reduce extends  Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			for (Text v: values) {
				count = count + 1;
				String line = v.toString();
				StringTokenizer tokenizer = new StringTokenizer(line, ",");
				String[] temp = new String[2];
				int i = 0;
				while (tokenizer.hasMoreTokens()) {
					temp[i] = tokenizer.nextToken().toString();
					i = i + 1;
				}
				//count = count + Integer.parseInt(v.toString().split(",")[0]);
				//count = count + Integer.parseInt(temp[0]); 
				double TransTotal = Double.parseDouble(temp[1]);
				min = Math.min(min, TransTotal);
				max = Math.max(max, TransTotal);
			}
			Text output = new Text();
			output.set(count + "  " + Double.toString(min) + "  " + Double.toString(max));
			context.write(key, output);
		}
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("query4");
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setJarByClass(Map.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		DistributedCache.addCacheFile(new URI("/user/hadoop/customers"), conf);
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
		
	}

}
