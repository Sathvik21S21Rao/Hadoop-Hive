import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoOccurrencePairs {

    public static class PairsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Set<String> frequentWords = new HashSet<>();
        private int windowDistance = 1;
        private Text pair = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get the window distance parameter from the configuration.
            windowDistance = context.getConfiguration().getInt("windowDistance", 1);

            // Load the frequent words file from the Distributed Cache.
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    // Open the file from the cache.
                    BufferedReader reader = new BufferedReader(new FileReader(new File(new Path(cacheFile.getPath()).getName())));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] tokens = line.split("\t");
                        if (tokens.length > 0) {
                            frequentWords.add(tokens[0].toLowerCase());
                        }
                    }                    
                    reader.close();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the line into words.
            String[] words = value.toString().split("\\s+");
            for (int i = 0; i < words.length; i++) {
                // Clean the word by removing punctuation and converting to lowercase.
                String currentWord = words[i].replaceAll("[^a-zA-Z]", "").toLowerCase();
                // Process only if current word is in the frequent words set.
                if (!frequentWords.contains(currentWord)) continue;
                // Determine the window boundaries.
                int start = Math.max(0, i - windowDistance);
                int end = Math.min(words.length - 1, i + windowDistance);
                // For each neighbor in the window (skipping the current word).
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    String neighborWord = words[j].replaceAll("[^a-zA-Z]", "").toLowerCase();
                    if (!frequentWords.contains(neighborWord)) continue;
                    // Create the pair key (format: "word,neighbor").
                    pair.set(currentWord + "," + neighborWord);
                    context.write(pair, one);
                }
            }
        }
    }

    public static class PairsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : values) {
                sum += count.get();
            }
            result.set(sum);
            // context.write(key, new IntWritable(sum));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: CoOccurrencePairs <input> <output> <frequentWordsFile> [windowDistance]");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        // Set the window distance if provided (default is 1)
        int windowDistance = 1;
        if (args.length == 4) {
            windowDistance = Integer.parseInt(args[3]);
        }
        conf.setInt("windowDistance", windowDistance);

        Job job = Job.getInstance(conf, "Co-occurrence pairs");
        job.setJarByClass(CoOccurrencePairs.class);
        job.setMapperClass(PairsMapper.class);
        job.setReducerClass(PairsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Add the frequent words file (top_freq_4a) to the Distributed Cache.
        job.addCacheFile(new URI(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
