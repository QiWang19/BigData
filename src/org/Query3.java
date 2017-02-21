package org;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query3 extends Configured implements Tool {
	
	public static List<ArrayList<Double>> getCentroid(String path){  
        List<ArrayList<Double>> res = new ArrayList<ArrayList<Double>>();  
        Configuration conf = new Configuration();  
        try {  
            FileSystem hdfs = FileSystem.get(conf);  
            Path in = new Path(path);  
            FSDataInputStream fsIn = hdfs.open(in);  
            LineReader lineIn = new LineReader(fsIn, conf);  
            Text line = new Text();  
            while (lineIn.readLine(line) > 0){  
                String record = line.toString();  
                String[] fields = record.split("\t");  
                String point = fields[1];
                String[] str = point.split(",");
                List<Double> l = new ArrayList<Double>();  
				l.add(Double.parseDouble(str[0]));
				l.add(Double.parseDouble(str[1]));
                res.add((ArrayList<Double>) l);  
            }  
            fsIn.close();  
        } catch (IOException e){  
            e.printStackTrace();  
        }  
        return res;  
    } 
    
    public static boolean unchange (String oldseed, String newseed) {
    	boolean res = true;
    	List<ArrayList<Double>> oldSeeds = getCentroid(oldseed);  
        List<ArrayList<Double>> newSeeds = getCentroid(newseed);
        for ( int i = 0; i < oldSeeds.size(); i++) {
        	double oldX = oldSeeds.get(i).get(0);
        	double oldY = oldSeeds.get(i).get(1);
        	double newX = newSeeds.get(i).get(0);
        	double newY = newSeeds.get(i).get(1);
        	double distance = Math.sqrt(Math.pow(newX - oldX, 2) + Math.pow(newY - oldY, 2));
        	if (distance < 0.000001 && distance > -0.000001) {
        		continue;
        	} else {
        		res = false; //change
        	}
        }
    	
    	return res;
    }
	

    
	public static int randomInt(int min, int max){
        Random random = new Random();
        int r = random.nextInt(max) % (max - min + 1) + min;
        return r;
	}
	
	public static void seedFile(int k) {
		int min = 0; 
		int max = 10000;
		int count = k;
		File csv = new File("/home/hadoop/Desktop/Project3/kMeans_seed.txt");
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
			
			for(int i = 0; i < count; i++){
				int x = randomInt(min, max);
				int y = randomInt(min, max);
				bw.write(String.valueOf(i + 1));
				bw.write("\t");
				bw.write(String.valueOf(x));
				bw.write(",");
				bw.write(String.valueOf(y));
				bw.newLine();
			}
			bw.flush();
			bw.close();
			System.out.println("Done!");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		private HashMap<String,String> h = new HashMap<String,String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws IOException {
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path:paths){
				if(path.toString().endsWith("kMeans_seed.txt")){
					BufferedReader br = new BufferedReader(new FileReader(path.toString()));
					String line=br.readLine();
					while(line != null) {
						String[] parts = line.split("\t");
						h.put(parts[0], parts[1]);
						line = br.readLine();
					}
				}
			}	
		}
		
		public void map(LongWritable key, Text value,Context context) 
				throws IOException, InterruptedException{
			
			String index = "-1" ;
			double min = Double.MAX_VALUE;
			
			String line = value.toString();
			String[] parts = line.split(",");
			int x = Integer.parseInt(parts[0]);
			int y = Integer.parseInt(parts[1]);
			
			for(String k: h.keySet()) {
				String seed_str = h.get(k);
				String[] seed_parts = seed_str.split(",");
				double seedX = Double.parseDouble(seed_parts[0]);
				double seedY = Double.parseDouble(seed_parts[1]);
				double d = Math.sqrt(Math.pow((x-seedX), 2)+Math.pow((y-seedY), 2));
				if (d < min) {
					index = k;
					min = d;
				}
			}
			String str[] = h.get(index).split(",");
			outputKey.set(str[0] +"," + str[1]);
			outputValue.set(line+","+Integer.toString(1));
			context.write(outputKey, outputValue);
			
		}
	}
	
	
	public static class Combine extends Reducer<Text, Text, Text, Text>{	

		private Text k = new Text();
		private Text v = new Text();
		
		public void combine(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			
			double sumX = 0;
			double sumY = 0;
			double count = 0;
			int x = 0;
			int y = 0;
			Iterator<Text> value = values.iterator();
			
			while (value.hasNext()) {
				String line = value.next().toString().trim();
				String[] str = line.split(",");
				x = Integer.parseInt(str[0]);
				y = Integer.parseInt(str[1]);
				sumX = sumX + x;
				sumY = sumY + y;
				count = count + 1;
			}
			
			k.set(key);
			v.set(Double.toString(sumX) + "," + Double.toString(sumY)+ "," + Double.toString(count));
			context.write(k, v);
			
		}
	}

	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{	

		private Text k = new Text();
		private Text v = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			
			double totalX = 0;
			double totalY = 0;
			double totalCount = 0;
			double meanX = 0;
			double meanY = 0;
			Iterator<Text> value = values.iterator();
			
			while (value.hasNext()) {
				String line = value.next().toString().trim();
				String[] str = line.split(",");
				double sumX = Double.parseDouble(str[0]);
				double sumY = Double.parseDouble(str[1]);
				double count = Double.parseDouble(str[2]);
				totalX = totalX + sumX;
				totalY = totalY + sumY;
				totalCount = totalCount + count;
			}
			
			meanX = (double)totalX/(double)totalCount;
			meanY = (double)totalY/(double)totalCount;

			String[] oldSeed = key.toString().trim().split(",");
			double x = Double.parseDouble(oldSeed[0]);
			double y = Double.parseDouble(oldSeed[1]);
			double distance = Math.sqrt(Math.pow(meanX - x, 2) + Math.pow(meanY - y, 2));
			String c = "change";
			if (distance < 0.000001 && distance > -0.000001) {
				c = "unchange";
			} else {
				c = "change";
			}
			
			k.set(key);
			v.set(Double.toString(meanX)+","+Double.toString(meanY) + "\t" + c);
			context.write(k, v);
			
		}
	}
	
	
	public int run(String[] args) throws Exception {
		
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DistributedCache.addCacheFile(new Path("/user/hadoop/Project3/kMeans_seed.txt").toUri(), conf);
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
	    job.setJobName("Q3");
	    job.setJarByClass(Query3.class);
		     
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Combine.class);
	    job.setReducerClass(Reduce.class);
	     
        job.setInputFormatClass(TextInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
               
	    job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;
	    
	}

	public static void main(String[] args) throws Exception {
		
		int iteration = 0;
		double dist = Double.MAX_VALUE;
		boolean tag = true;
		
		int k = Integer.parseInt(args[2]);
		double threshold = Double.parseDouble(args[3]);
		seedFile(k); // Generate local file kMeans_seed.txt
		Configuration cf = new Configuration();
		FileSystem fs = FileSystem.get(cf);
		fs.moveFromLocalFile(new Path("/home/hadoop/Desktop/Project3/kMeans_seed.txt"), 
								new Path("/user/hadoop/Project3/kMeans_seed.txt"));  // put local file to HDFS
		boolean flag = false;
		do {
			int returnCode = ToolRunner.run(new Query3(), args);

			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			
			Path oldseed = new Path("/user/hadoop/Project3/kMeans_seed.txt");
			Path newseed = new Path("/user/hadoop/Project3/output_problem3/part-r-00000");
			
			flag = unchange(oldseed.toString(), newseed.toString());

			hdfs.copyToLocalFile(new Path("/user/hadoop/Project3/output_problem3/part-r-00000"), 
									new Path("/home/hadoop/Desktop/Project3/Q3out.txt"));
			hdfs.copyToLocalFile(new Path("/user/hadoop/Project3/output_problem3/part-r-00000"), 
									new Path("/home/hadoop/Desktop/Project3/Q3out" + Integer.toString(iteration) + ".txt"));
			hdfs.delete(new Path("/user/hadoop/Project3/kMeans_seed.txt"),true);
			hdfs.moveFromLocalFile(new Path("/home/hadoop/Desktop/Project3/Q3out.txt"),
									new Path("/user/hadoop/Project3/kMeans_seed.txt"));
			hdfs.delete(new Path(args[1]), true);
			
		        //Delete local file "~/Q3out/part-r-00000"
			File index = new File("/home/hadoop/Desktop/Project3/Q3out.txt");
			index.delete();

			iteration = iteration + 1;
			System.out.println("Repeated:" + iteration);
			System.out.println(flag);
		} while (iteration < 5 && flag == false ); // dist > Threshold
		
		
	}
	
}

