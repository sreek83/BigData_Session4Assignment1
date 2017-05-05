package test.hadoop.session4.assig1.task1;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class TeleDataMap extends Mapper<LongWritable, Text, NullWritable, Text> {

	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
	

		 String [] strTokens = value.toString().split("\\|");
		 
		 if(!strTokens[0].equals("NA") && !strTokens[1].equals("NA"))
		{
			 System.out.println("write");
			context.write(NullWritable.get(), value);
		 }
		 
	}
}
