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

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// inputKey: user_id     inputValue: movie_id:rate, movie_id:rate, ...
			// value: user_id \t movie1_rate:rate, movie2:rate, ...
			// outputKey: movie1:movie2      outputValue: 1

			String[] user_movie_rate = value.toString().trim().split("\t");
			String[] movie_rate = user_movie_rate[1].split(",");

			for (int i = 0; i < movie_rate.length; i++) {
				String movie1 = movie_rate[i].trim().split(":")[0];

				for (int j = 0; j < movie_rate.length; j++) {
					String movie2 = movie_rate[j].trim().split(":")[0];
					Text outputKey = new Text(movie1 + ":" + movie2);

					context.write(outputKey, new IntWritable(1));
				}
			}
			
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// inputKey: movie1:movie2     inputValue: iterable<1, 1, 1, ...>
			int sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			context.write(key, new IntWritable(sum));

		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
