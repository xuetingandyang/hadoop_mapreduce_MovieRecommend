import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// value: movieB \t movieA = nomarlized_relation
			String[] line = value.toString().trim().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// value: user, movie, rate
			// output: movie -> user:rate
			String[] line = value.toString().trim().split(",");
			context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// inputKey: movieB
			// inputValue: iterable< movieA=relationA, movieC=relationA, ..., userA:rateA, userB:rateB,... >
			// output: userA:movieA->rateA*relationA, userA:movieC-> rateA*relationC, ...

			Map<String, Double> relationMap = new HashMap<>();
			Map<String, Double> rateMap = new HashMap<>();

			for (Text value: values) {
				if (value.toString().contains("=")) {
					String[] movie_relation = value.toString().split("=");
					String movie = movie_relation[0];
					Double relation = Double.parseDouble(movie_relation[1]);

					relationMap.put(movie, relation);
				} else {
					String[] user_rate = value.toString().split(":");
					String user = user_rate[0];
					Double rate = Double.parseDouble(user_rate[1]);

					rateMap.put(user, rate);
				}
			}

			for (Map.Entry<String, Double> relationPair: relationMap.entrySet()) {

				for (Map.Entry<String, Double> ratePair: rateMap.entrySet()) {
					String outputKey = ratePair.getKey() + ":" + relationPair.getKey();
					Double outputValue = ratePair.getValue() * relationPair.getValue();

					context.write(new Text(outputKey), new DoubleWritable(outputValue));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
