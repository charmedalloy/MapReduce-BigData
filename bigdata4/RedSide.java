package aval.bigdata4;

import java.io.IOException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.Collections;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class RedSide {

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\t");
			String person = mydata[0];

			if (mydata.length == 2) {

				List<String> friends = new ArrayList<>();

				StringTokenizer tokenizer = new StringTokenizer(mydata[1], ",");
				while (tokenizer.hasMoreTokens()) {

					String temp = tokenizer.nextToken();
					friends.add(temp);
				}

				for (String f : friends) {
					context.write(new Text(f), new Text(person + "A")); // (2,1A)
																		// (3,1A)
																		// (4,1A)
				}
			}
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split(",");
			String userid = mydata[0];

			if (mydata.length == 10) {

				context.write(new Text(userid), new Text(String.valueOf(calAge(mydata[9])) + "B")); // (1,20B)
																									// (2,40B)..

			}

		}

		public int calAge(String dob) {

			DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
			Date d;
			try {
				d = df.parse(dob);
				Date current = new Date();
				int age = (int) ((current.getTime() - d.getTime()) / (1000 * 24 * 60 * 60));
				return (age / 365);

			} catch (Exception e) {

			}
			return 0;
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		HashMap<String, Integer> hmap = new HashMap<>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String id = "";
			int age = 0;

			for (Text val : values) {
				if (val.toString().contains("A")) {
					id = val.toString().substring(0, val.toString().length() - 1);
				} else {
					age = Integer.parseInt(val.toString().substring(0, val.toString().length() - 1));
				}
			}
			if (hmap.containsKey(id)) {
				hmap.put(id, Math.max(hmap.get(id), age));
			} else {
				hmap.put(id, age);
			}

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, Integer> entry : hmap.entrySet()) {
				context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
			}
		}
	}

	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> { // user
																				// id
																				// and
																				// address,city,state

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] mydata = value.toString().split(",");
			if (mydata.length == 10) {
				context.write(new Text(mydata[0]), new Text(mydata[3] + "," + mydata[4] + "," + mydata[5]));
			}
		}
	}

	public static class Map4 extends Mapper<LongWritable, Text, Text, Text> { // user
																				// id
																				// and
																				// age

		LinkedList<String> list = new LinkedList<>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\t");
			String userId = mydata[0];
			String maxAge = mydata[1];
			String temp = maxAge + " " + userId;
			list.add(temp);

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

			Collections.sort(list, Collections.reverseOrder());
			for (int i = 0; i <= 10; i++) {
				String temp = list.get(i);
				String[] result = temp.split(" ");
				context.write(new Text(result[1]), new Text(result[0] + "**"));
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder result = new StringBuilder("");
			String age = "";
			String address = "";
			int count = 0;
			for (Text val : values) {

				if (val.toString().contains("**")) {
					age = val.toString().substring(0, val.toString().length() - 2);
				} else {
					address = val.toString();
				}

				count++;
			}
			result.append("," + address + "," + age);
			if (count == 2)
				context.write(new Text(key.toString() + (result.toString())), new Text());
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length < 3) {
			System.err.println("Usage: <friends list> <user info> <out> ");
			System.exit(2);
		}

		// create a job with name

		Job job1 = Job.getInstance(conf, "RedFriends");
		job1.setJarByClass(RedSide.class);
		job1.setMapperClass(Map1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, Map2.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(Reduce.class);
		job1.setNumReduceTasks(1);
		String job1OutPath = otherArgs[2] + "/job1";
		FileOutputFormat.setOutputPath(job1, new Path(job1OutPath));

		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(new Configuration(), "RedFriends2");
		job2.setJarByClass(RedSide.class);
		job2.setMapperClass(Map3.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, Map3.class);
		MultipleInputs.addInputPath(job2, new Path(job1OutPath), TextInputFormat.class, Map4.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(Reduce2.class);
		job2.setNumReduceTasks(1);
		String job2OutPath = otherArgs[2] + "/job2";
		FileOutputFormat.setOutputPath(job2, new Path(job2OutPath));
		job2.waitForCompletion(true);
	}

}
