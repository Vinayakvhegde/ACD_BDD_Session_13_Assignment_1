import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class DistributorByVolume {
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		if (args.length != 2) 
		{
			System.err.println("Usage: stdsubscriber <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Assignment 1 :Petrol Distributor by Sold Volume");
		job.setJarByClass(DistributorByVolume.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		Path out = new Path(args[1]) ;
		out.getFileSystem(conf).delete(out);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class SumReducer 
		extends	Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
	
		public void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
						throws IOException, InterruptedException {
			long  totalVolumeSold = 0 ;
			for (LongWritable val : values) {
				totalVolumeSold += (val.get());
			}
			this.result.set(totalVolumeSold);
			context.write(key, this.result);
		}
	}

	public static class TokenizerMapper 
		extends	Mapper<Object, Text, Text, LongWritable> 
	{
		Text distributor = new Text();
		LongWritable volumeSold = new LongWritable();
		public void map(Object key, Text value,	Mapper<Object, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException 
		{
			String[] parts = value.toString().split(",");
			distributor.set(parts[CDRConstants.distName]);
			long volumeOut = Long.parseLong(parts[CDRConstants.volumeOut]);
			volumeSold.set(volumeOut);
			context.write(distributor, volumeSold);
		}
	}
		
}