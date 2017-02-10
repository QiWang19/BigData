package hw1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class Query4_new extends Configured implements Tool{
	public static class MapperMapSideJoinDCacheTextFile extends
		Mapper<LongWritable, Text, Text, Text> {
		private  HashMap<String, String> Customers = new HashMap<String, String>();
		private BufferedReader brReader;
		private Text outKey = new Text("");
		private Text outValue = new Text("");
		float maxTrans ;
		float minTrans ;
		float transTotal;
		
		@Override
		protected void setup(Context context) throws IOException,
			InterruptedException {
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String customerID = null;
				String CountryID = null;
				for (Path eachPath : paths) {
					if (eachPath.getName().toString().trim().equals("customers")) {
						brReader = new BufferedReader(new FileReader(eachPath.toString()));
						while(null !=(customerID = brReader.readLine())){
							String[] splits = customerID.split(",");
	//						put countrycode and customerId in hashmap
							Customers.put(splits[0],splits[3] );
						}
					}
				}
		}
			catch (IOException e)  
            {  
                e.printStackTrace();  
            }  
            finally  
            {  
                try  
                {  
                	brReader.close();  
                }  
                catch (IOException e)  
                {  
                    e.printStackTrace();  
                }  
            } 
		}
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  
        {  
              
            String[] order = value.toString().split(",");  
            String countryCode = Customers.get(order[1]);  
			transTotal = Float.parseFloat(order[2]);

              
            if(countryCode != null)  
            {  
                outKey.set(countryCode);  
                outValue.set("1," +  transTotal);  
                context.write(outKey, outValue);;  
            }  
        } 

	}
	
	public static class selectReducer 
  		extends Reducer<Text, Text,Text,Text> {
	  	float transTotal;
	  	
	  	float maxTrans;
	  	float minTrans;
	  
	  public void reduce(Text countryID, Iterable<Text> values , 
                  Context context
                  ) throws IOException, InterruptedException {
		  		int count=0;
		   
			  maxTrans = 10;
			  minTrans = 1000;
		  for (Text val : values) {
			  
			  count = count+Integer.parseInt(val.toString().split(",")[0]);
			  transTotal = Float.parseFloat(val.toString().split(",")[1]);
			   if (transTotal<minTrans){
				   minTrans=transTotal;}
			   if (transTotal>maxTrans){
				   maxTrans=transTotal;
			  }
			
		  }
		  
		  Text results = new Text();
		  results.set((count+"  "+Float.toString(minTrans)+"  "+Float.toString(maxTrans)));
		  context.write(countryID,results);
	}
}	
	 

	public static void main(String[] args) throws Exception  
    {  
        int res = ToolRunner.run(new Configuration(), new Query4_new(), args);  
        System.exit(res);  
    }  


	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
        job.setJobName("Query4_new");  
        job.setMapperClass(MapperMapSideJoinDCacheTextFile.class);  
        job.setReducerClass(selectReducer.class);  
		job.setJarByClass(MapperMapSideJoinDCacheTextFile.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(Text.class);  
          
        DistributedCache.addCacheFile(new URI("/user/hadoop/customers"), conf);  
        FileInputFormat.setInputPaths(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
  
        boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;    
	}

	
}
