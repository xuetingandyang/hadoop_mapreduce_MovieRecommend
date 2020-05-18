import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataDividerByUser {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input: user_id,movie_id,rating;
			// output: Key: user_id  Value: Movie_id:Rating

			String[] user_movie_rate = value.toString().trim().split(",");
			int user_id = Integer.parseInt(user_movie_rate[0]);
			Text movie_rate = new Text(user_movie_rate[1] + ":" + user_movie_rate[2]);

			context.write(new IntWritable(user_id), movie_rate);

		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// inputKey: user_id, inputValue: movie_id:rating
			// outputKey: user_id, outputValue: movie_id:rating, movie_id:rating, ...

			StringBuilder sb = new StringBuilder();
			while (values.iterator().hasNext()) {
				sb.append("," + values.iterator().next());
			}
			Text movie_rates = new Text(sb.toString().replaceFirst(",",""));

			context.write(key, movie_rates);

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDividerByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
