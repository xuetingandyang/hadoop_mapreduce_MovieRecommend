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

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value: movie1:movie2 \t relation
            // output: movie1 -> movie2:relation

            String[] movies_relation = value.toString().trim().split("\t");
            String[] movies = movies_relation[0].split(":");
            Text outputKey = new Text(movies[0]);
            Text outputValue = new Text(movies[1] + ":" + movies_relation[1]);

            context.write(outputKey, outputValue);

        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // input: movie1 -> iterable<movie2:relation, movie3:relation,...>
            // output: movie2 -> relation/sum, movie3 -> relation/sum, ...

            int sum = 0;
            Map<String, Integer> pairs = new HashMap<>();
            while (values.iterator().hasNext()) {
                String[] movie_relation = values.iterator().next().toString().trim().split(":");
                int relation = Integer.parseInt(movie_relation[1]);
                pairs.put(movie_relation[0], relation);

                sum += relation;
            }

            for (Map.Entry<String, Integer> pair: pairs.entrySet()) {
                Text outputKey = new Text(pair.getKey());
                Double normalized_relation = (double)pair.getValue() / sum;
                Text outputValue = new Text(key + "=" + normalized_relation);

                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
