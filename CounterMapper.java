package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public  class CounterMapper extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > 
{
	 private String one = new String();
	 
	 @Override
	 public void map(LongWritable key,  Text values, Context context) throws  IOException,  InterruptedException
	 {
		   context.write(new Text(one), new IntWritable(1));    //Mapper will produce the output 1 for each page 
	 }
}