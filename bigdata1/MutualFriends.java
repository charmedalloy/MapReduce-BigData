package aval.bigdata1;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {

	/*
	 * 
	 * This class emits a key(pair of 2 friends)  and value ( list of friends )
	 * 
	 */
	public static class Map extends Mapper<LongWritable, Text, PairWritable, Text> { //(1,2)->frendsList of 1
																					 //(1,3)-> friendsList of 1	

		public void map(LongWritable key, Text list, Context context) throws IOException, InterruptedException {

			String[] mydata = list.toString().split("\t");
			if (mydata.length == 2) {
				Text friendList = new Text();
				StringBuilder sb = new StringBuilder("");
				Long person = Long.parseLong(mydata[0]);
				List<Long> friends = new ArrayList<>();

				StringTokenizer tokenizer = new StringTokenizer(mydata[1], ",");
				while (tokenizer.hasMoreTokens()) {

					String temp = tokenizer.nextToken();
					Long friend = Long.parseLong(temp);
					friends.add(friend);
					sb.append(temp);
					sb.append(",");
				}

				friendList.set(sb.toString());

				for (Long f : friends) {

					context.write(sortPair(person, f), friendList);
				}
			}
		}
	}

	public static PairWritable sortPair(Long p1, Long p2) { //sorting of (2,1) to (1,2)

		if (p1 > p2) {
			return (new PairWritable(p2, p1));

		} else
			return (new PairWritable(p1, p2));

	}

	/*
	 * 
	 * This class would get a key(pair of friends) and value(2 lists of friends).
	 * To find the mutual friends, find the intersection of the 2 lists obtained as value
	 * 
	 */
	public static class Reduce extends Reducer<PairWritable, Text, PairWritable, Text> {

		public void reduce(PairWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			boolean mutualExist = false; 

			List<String> mutualFriends = new ArrayList<>();
			int flag = 0;
			StringBuilder resultList = new StringBuilder("");
			for (Text val : values) {

				if (flag == 0) {
					// mutualExist = false;
					String[] temp = val.toString().split(",");
					for (int i = 0; i < temp.length; i++) {
						mutualFriends.add(temp[i]);
					}
					flag = 1;
				} else {
					mutualExist = true;
					String[] temp = val.toString().split(",");
					List<String> tempList = new ArrayList<>();
					for (int i = 0; i < temp.length; i++) {
						tempList.add(temp[i]);
					}
					mutualFriends.retainAll(tempList);

				}
			}

			for (String a : mutualFriends) {
				resultList.append(a);
				resultList.append(" ");
			}

			Text result = new Text(resultList.toString());
			if (mutualExist && mutualFriends.size() > 0)
				context.write(key, result);

		}

	}

	// Driver program
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		// create a job with name
		Job job = Job.getInstance(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(Text.class);
		// set output key type
		job.setOutputKeyClass(PairWritable.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
