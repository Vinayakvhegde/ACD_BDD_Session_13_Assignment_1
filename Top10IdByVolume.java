import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Top10IdByVolume {
	public static void main(String[] args) throws Exception 
	{
		Configuration conf1 = new Configuration();
		if (args.length != 2) 
		{
			System.err.println("Usage: stdsubscriber <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf1, "Assignment 1: Top 10 Petrol Distributor ID by Sold Volume");
		job1.setJarByClass(Top10IdByVolume.class);
		
		job1.setMapperClass(TokenizerMapper.class);
		job1.setMapOutputValueClass(LongWritable.class);
		
		job1.setReducerClass(SumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		
		Path out1 = new Path(args[1]) ;
		out1.getFileSystem(conf1).delete(out1);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, out1);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}

	public static class SumReducer 
		extends	Reducer<Text, LongWritable, LongWritable, Text> {
		private LongWritable result = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, LongWritable, Text>.Context context)
						throws IOException, InterruptedException 
		{
			long  totalVolumeSold = 0 ;
			for (LongWritable val : values) {
				totalVolumeSold += (val.get());
			}
			this.result.set(totalVolumeSold);
			context.write(this.result, key);			
		}
	}

	public static class TokenizerMapper 
		extends	Mapper<Object, Text, Text, LongWritable> 
	{
		Text distId = new Text();
		LongWritable volumeSold = new LongWritable();
		public void map(Object key, Text value,	Mapper<Object, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException 
		{
			String[] parts = value.toString().split(",");
			distId.set(parts[CDRConstants.distId]);
			long volumeOut = Long.parseLong(parts[CDRConstants.volumeOut]);
			volumeSold.set(volumeOut);
			context.write(distId, volumeSold);
		}
	}
		
}

