package org;

import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper.Context;
import org.apache.hadoop.util.Tool;

public class Problem3 extends Configured implements Tool {
	private static ArrayList<ArrayList<Double>> l = new ArrayList<ArrayList<Double>>();
	public static class Map implements Mapper<LongWritable, Text, Text, Text> {
		private HashMap<String, String> h = new HashMap<String, String>();
		
		private BufferedReader br;
		public void setup(Context context) throws IOException {
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path: paths) {
				if (path.getName().toString().trim().equals("seed")) {
					br = new BufferedReader(new FileReader(path.toString()));
					String line = br.readLine();
					while (line != null) {
						ArrayList<Double> temp = new ArrayList<Double>();
						String[] splits = line.split(",");
						temp.add(Double.parseDouble(splits[1]));
						temp.add(Double.parseDouble(splits[2]));
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
			
			Text k = new Text();
			Text v= new Text();
			k.set(String.valueOf(index));
			v.set(String.valueOf(x) + "," + String.valueOf(y));
			context.write(k, v);
			
		}
		
		public double distance(double x1, double y1, double x2, double y2) {
			double d = Math.sqrt(Math.pow((x1 - x2), 2) + Math.pow((y1 - y2), 2));
			return d;
		}

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
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
			
			Text k = new Text();
			Text v= new Text();
			k.set(String.valueOf(index));
			v.set(String.valueOf(x) + "," + String.valueOf(y));
			//context.write(k, v);
			output.collect(k, v);
			
			
		}
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
			
			Text k = new Text();
			Text v = new Text();
			k.set(key);
			v.set(Double.toString(meanX) + "," + Double.toString(meanY));
			context.write(k, v);
		}
	}

	public static void main(String[] args) {
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
