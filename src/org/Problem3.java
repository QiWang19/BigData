package org;

import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
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
import org.apache.hadoop.util.*;

public class Problem3 extends Configured implements Tool {
	
	private static ArrayList<ArrayList<Double>> l = new ArrayList<ArrayList<Double>>();
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private HashMap<String, String> h = new HashMap<String, String>();
		
		private BufferedReader br;
		public void setup(Context context) throws IOException {
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path: paths) {
				if (path.getName().toString().trim().equals("seed")) {
					br = new BufferedReader(new FileReader(path.toString()));
					String line = br.readLine();
					while (line != null) {
						String[] parts = line.split("\t");
						ArrayList<Double> temp = new ArrayList<Double>();
						String[] splits = parts[1].split(",");
						temp.add(Double.parseDouble(splits[0]));
						temp.add(Double.parseDouble(splits[1]));
						l.add(temp);
						line = br.readLine();
					}
				}
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(",");
			Double x = Double.parseDouble(str[1]);
			Double y = Double.parseDouble(str[2]);
			Double min = Double.MAX_VALUE;
			int index = -1;
			for (int i = 0; i < l.size(); i++) {
				double d = distance(x, y, l.get(i).get(0), l.get(i).get(1));
				if (d <= min) {
					min = d;
					index = i;
				}
			}
			if (String.valueOf(x) + "," + String.valueOf(y) != null) {
				Text k = new Text();
				Text v= new Text();
				k.set(l.get(index).get(0).toString() + "," + l.get(index).get(1).toString());
				//k.set(String.valueOf(index));
				v.set(String.valueOf(x) + "," + String.valueOf(y));
				context.write(k, v);
			}
			
			
		}
		
		

		

//		public void map(LongWritable key, Text value, Context context)
//				throws IOException {
//			String[] str = value.toString().split(",");
//			Double x = Double.parseDouble(str[1]);
//			Double y = Double.parseDouble(str[2]);
//			Double min = Double.MAX_VALUE;
//			int index = -1;
//			for (int i = 0; i < l.size(); i++) {
//				double d = distance(x, y, l.get(i).get(0), l.get(i).get(1));
//				if (d <= min) {
//					min = d;
//					index = i;
//				}
//			}
//			
//			Text k = new Text();
//			Text v= new Text();
//			k.set(String.valueOf(index));
//			v.set(String.valueOf(x) + "," + String.valueOf(y));
//			context.write(k, v);
//			//output.collect(k, v);
//			
//			
//		}
	}
	
	public class Combine extends  Reducer<Text, Text, Text, Text> {
		public void combine(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sumX = 0;
			double sumY = 0;
			int count = 0;
			Iterator<Text> value = values.iterator();
			while (value.hasNext()) {
				String line = value.next().toString().trim();
				String[] str = line.split(",");
				int x = Integer.parseInt(str[0]);
				int y = Integer.parseInt(str[1]);
				sumX = sumX + x;
				sumY = sumY + y;
				count = count + 1;
			}
			Text k = new Text();
			Text v = new Text();
			k.set(key);
			v.set(Double.toString(sumX) + "," + Double.toString(sumY) + "," + Integer.toString(count));
			context.write(k, v);
		}
	}
	
	public static class Reduce extends  Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double totalX = 0;
			double totalY = 0;
			int total = 0;
			for (Text v: values) {
				String line = v.toString().trim();
				String[] str = line.split(",");
				double sumX = Double.parseDouble(str[0]);
				double sumY = Double.parseDouble(str[1]);
				int count = Integer.parseInt(str[2]);
				totalX = totalX + sumX;
				totalY = totalY + sumY;
				total = total + count;
			}
			double meanX = totalX / total;
			double meanY = totalY / total;
			
			String[] keys = key.toString().split(",");
			double temp = distance(Double.parseDouble(keys[0]), Double.parseDouble(keys[1]), meanX, meanY);
			String state;
			if (temp > -0.000001 && temp < 0.000001) {
				state = "unchange";
			} else {
				state = "change";
			}
			
			Text k = new Text();
			Text v = new Text();
			k.set(key);
			v.set(Double.toString(meanX) + "," + Double.toString(meanY) + "," + state);
			context.write(k, v);
		}
	}
	
	public static double distance(double x1, double y1, double x2, double y2) {
		double d = Math.sqrt(Math.pow((x1 - x2), 2) + Math.pow((y1 - y2), 2));
		return d;
	}
	
	public static ArrayList<ArrayList<Double>> getCentroid (String path) {
		ArrayList<ArrayList<Double>> res = new ArrayList<ArrayList<Double>>();
		Configuration conf = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(conf);
			Path input = new Path(path);
			FSDataInputStream fsInput = hdfs.open(input);
			LineReader lineIn = new LineReader(fsInput, conf);
			Text line = new Text();
			while (lineIn.readLine(line) > 0) {
				String record = line.toString().trim();
				String[] str = record.split("\t");
				//String points = str[1];
				String[] points = str[1].split(","); 
				Double x = Double.parseDouble(points[0]);
				Double y = Double.parseDouble(points[1]);
				ArrayList<Double> point = new ArrayList<Double>();
				point.add(x);
				point.add(y);
				res.add(point);
			}
			fsInput.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res;
	}

	public static void main(String[] args) throws Exception {
		int iteration = 0;
		//String centroidPath = "";
		boolean flag = true;
		//Configuration conf = new Configuration();
		
		//String oldCentroidPath = centroidPath;
		//String currentCentroidPath = centroidPath;
		
		do {
			int returnCode = ToolRunner.run(new Configuration(), new Problem3(), args);
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			
			//TODO cache file path old seed
			Path oldseed = new Path("/usr/hadoop/seed");
			//TODO new meanX, Y file
			Path newseed = new Path("/usr/hadoop/output10/part-r-00000");
			
			
			ArrayList<ArrayList<Double>> oldS = getCentroid(oldseed.toString());
			ArrayList<ArrayList<Double>> newS = getCentroid(newseed.toString());
			for (int i = 0; i < oldS.size(); i++) {
				flag = true;
				double temp = distance( oldS.get(i).get(0), oldS.get(i).get(1), newS.get(i).get(0), newS.get(i).get(1));
				if (temp> -0.000001 && temp < 0.000001) {
					continue;
				} else {
					flag = false;
				}
						
			}
			
			//TODO new meanXY to home/....
			hdfs.copyToLocalFile(new Path("/usr/hadoop/output10/part-r-00000"), new Path("/home/hadoop/mean"));
			
			//TODO delete cache path file
			hdfs.delete(new Path("/usr/hadoop/seed.csv"), true);
			//TODO home...in 226 to cache path
			hdfs.moveFromLocalFile(new Path("/home/hadoop/mean.csv"), new Path("/usr/hadoop/seed.csv"));
			
			hdfs.delete(new Path(args[1]), true);
			
			//conf.set("centroid.path", currentCentroidPath);
		} while (iteration < 5 && flag == false);
		
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DistributedCache.addCacheFile(new Path("/usr/hadoop/seed").toUri(), conf);
		
	    job.setJobName("p3");
	    job.setJarByClass(Problem3.class);
		     
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setMapperClass(Map.class);
	    job.setNumReduceTasks(5);
	     
//        job.setInputFormatClass(TextInputFormat.class);   
//        job.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
               
	    job.waitForCompletion(true);
		
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		//TODO cache path
		DistributedCache.addCacheFile(new Path("/usr/hadoop/seed").toUri(), conf);
		job.setJobName("Kmeans");
		job.setJarByClass(Problem3.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        return job.isSuccessful()?0:1;
		
		
	}

}
