package aval.bigdata3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendsData {

	public static class Map extends Mapper<LongWritable, Text, PairWritable, Text> {

		public void map(LongWritable key, Text list, Context context) throws IOException, InterruptedException {
			Text friendList = new Text();
			StringBuilder sb = new StringBuilder("");
			String[] mydata = list.toString().split("\t");
			Long person = Long.parseLong(mydata[0]);
			String userId1 = context.getConfiguration().get("UserId1");
			String userId2 = context.getConfiguration().get("UserId2");
			if (mydata.length == 2 && (person == Long.parseLong(userId1) || person == Long.parseLong(userId2))) {

				List<Long> friends = new ArrayList<>();
				StringTokenizer tokenizer = new StringTokenizer(mydata[1], ",");
				while (tokenizer.hasMoreTokens()) {

					String temp = tokenizer.nextToken();
					Long friend = Long.parseLong(temp);
					friends.add(friend);
					sb.append(temp);
					sb.append(",");
				}
				sb.setLength(sb.length() - 1);
				friendList.set(sb.toString());

				for (Long f : friends) {
					context.write(sortPair(person, f), friendList);
				}

			}
		}

		public static PairWritable sortPair(Long p1, Long p2) {

			if (p1 > p2) {
				return (new PairWritable(p2, p1));

			} else
				return (new PairWritable(p1, p2));
		}

	}

	public static class Reduce extends Reducer<PairWritable, Text, PairWritable, Text> {
		HashMap<String, String> hmap = new HashMap<>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("userdataFilePath");
			Path pt = new Path("hdfs://" + filePath);// Location of
														// file in HDFS
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				hmap.put(arr[0], "[ " + arr[1] + " " + arr[2] + ":" + arr[9] + " ]");
				line = br.readLine();
			}
		}

		public void reduce(PairWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			boolean mutualExist = false;

			List<String> mutualFriends = new ArrayList<>();
			int flag = 0;
			// StringBuilder resultList = new StringBuilder("");
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
			if (mutualExist && mutualFriends.size() > 0) {
				StringBuilder builder = new StringBuilder("");
				for (String f : mutualFriends) {
					if (hmap.containsKey(f)) {
						builder.append(hmap.get(f));
						builder.append(",");
					}
				}
				builder.setLength(builder.length() - 1);
				Text result = new Text(builder.toString());

				context.write(key, result);
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length < 5) {
			System.err.println("Usage: <in> <out> <path> <userid1> <userid2>");
			System.exit(2);
		}

		conf.set("userdataFilePath", otherArgs[2]);
		conf.set("UserId1", otherArgs[3]);
		conf.set("UserId2", otherArgs[4]);
		// create a job with name
		Job job = Job.getInstance(conf, "MutualFriends");

		job.setJarByClass(FriendsData.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

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
