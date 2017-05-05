package test.hadoop.session4.assig1.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

public class TeleData {

	public static void main(String[] args) throws IOException, InterruptedException,ClassNotFoundException {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "teleData");
		
		job.setMapperClass(TeleDataMap.class);
	

		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1); 

	}

}
