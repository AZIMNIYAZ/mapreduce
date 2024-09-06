package in.project.mapreduce;

import java.io.IOException; 
import java.util.StringTokenizer; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemp { 
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    	Text year = new Text(); 

        @Override 
        public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
        	String line = value.toString(); 
            StringTokenizer tokenizer = new StringTokenizer(line," "); 

            while (tokenizer.hasMoreTokens()) { 
            	String yearStr = tokenizer.nextToken();
            	year.set(yearStr);
            	int temp = Integer.parseInt(tokenizer.nextToken().trim()); 
                context.write(year, new IntWritable(temp)); 
            } 
        } 
    } 

    
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override 
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
        	int maxTemp = Integer.MIN_VALUE;
            for(IntWritable temp : values) {
            	maxTemp = Math.max(maxTemp, temp.get());
            } 
            context.write(key, new IntWritable(maxTemp)); 
        } 
    } 
    
    public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MaxTemp");
		job.setJarByClass(MaxTemp.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
