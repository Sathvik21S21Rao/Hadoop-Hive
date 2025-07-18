import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class TopFreq {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = false;
		private Set<String> patternsToSkip = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);

			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			for (URI patternsURI : patternsURIs) {
				Path patternsPath = new Path(patternsURI.getPath());
				String patternsFileName = patternsPath.getName().toString();
				parseSkipFile(patternsFileName);
			}
		}

		private void parseSkipFile(String fileName) {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while ((pattern = reader.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
				reader.close();
			} catch (IOException ioe) {
				System.err.println(
						"Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			String[] tokens = line.split("[^\\w']+");
			for (String token : tokens) {
				if (!token.isEmpty() && !patternsToSkip.contains(token)) {
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private Map<String, Integer> wordCounts = new HashMap<String, Integer>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			wordCounts.put(key.toString(), sum);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(
					(a, b) -> a.getValue().equals(b.getValue())
							? a.getKey().compareTo(b.getKey())
							: b.getValue() - a.getValue());
			pq.addAll(wordCounts.entrySet());

			int count = 0;
			while (!pq.isEmpty() && count < 50) {
				Map.Entry<String, Integer> entry = pq.poll();
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
				count++;
			}

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top50freq");

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local
		// aggregation'
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < args.length; ++i) {
			if ("-stopwords".equals(args[i])) {
				// job.getConfiguration().setBoolean("wordcount.skip.patterns", true);

				job.addCacheFile(new Path(args[++i]).toUri());
			} else if ("-casesensitive".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.case.sensitive", true);
			}
		}

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}