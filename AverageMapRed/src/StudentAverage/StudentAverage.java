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
	
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	         String line = value.toString();
	         String[] word = line.split(",");
	         Text outputKey = new Text(word[0].toUpperCase().trim());
	         FloatWritable outputValue = new FloatWritable(Float.parseFloat(word[1]));
	         context.write(outputKey, outputValue);
	         //StringTokenizer tokenizer = new StringTokenizer(line, ",");
	         //while(tokenizer.hasMoreTokens()) {
	        	 //context.write(new Text(tokenizer.nextToken()), new FloatWritable(Float.parseFloat(tokenizer.nextToken())));
	         //}
	      }
	   }

	   public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	      public void reduce(Text studentName, Iterable<FloatWritable> marks, Context context) throws IOException, InterruptedException {
	         float total = 0;
	         int numOfSubjects = 0;
	         for (FloatWritable mark : marks) {
	        	 total += mark.get();
	        	 numOfSubjects++;
	         }
	         float average = total/numOfSubjects;
	         context.write(studentName, new FloatWritable(average));
	      }
	   }

	   public static void main(String[] args) throws Exception {
	      Configuration conf = new Configuration();

	      Job job = new Job(conf, "studentaverage");

	      job.setJarByClass(StudentAverage.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(FloatWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	   }

}
