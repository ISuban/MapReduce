package StudentAverage;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StudentAverage {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	         String line = value.toString();
	         StringTokenizer tokenizer = new StringTokenizer(line);
	         context.write(new Text(tokenizer.nextToken()), new IntWritable(Integer.parseInt(tokenizer.nextToken())));
	      }
	   }

	   public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	      public void reduce(Text studentName, Iterable<IntWritable> marks, Context context)
	         throws IOException, InterruptedException {
	         int total = 0;
	         int numOfSubjects = 0;
	         for (IntWritable mark : marks) {
	        	 total += mark.get();
	        	 numOfSubjects++;
	         }
	         context.write(studentName, new FloatWritable(total/numOfSubjects));
	      }
	   }

	   public static void main(String[] args) throws Exception {
	      Configuration conf = new Configuration();

	      Job job = new Job(conf, "studentaverage");

	      job.setJarByClass(StudentAverage.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	   }

}
