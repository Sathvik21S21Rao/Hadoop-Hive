import java.io.*;
import java.net.URI;
import java.util.*;
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

public class PairsAggregation {

    public static class PairsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Set<String> frequentWords = new HashSet<>();
        private int windowDistance = 1;
        private int aggregationLevel = 1;
        private Text pair = new Text();
        private final static IntWritable one = new IntWritable(1);
        private HashMap<String, Integer> localCounts = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get the window distance and aggregation level from the configuration.
            windowDistance = context.getConfiguration().getInt("windowDistance", 1);
            aggregationLevel = context.getConfiguration().getInt("aggregationLevel", 1);

            // Load the frequent words file from the Distributed Cache.
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    BufferedReader reader = new BufferedReader(
                            new FileReader(new File(new Path(cacheFile.getPath()).getName())));
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
            HashMap<String, Integer> lineLocalCounts = new HashMap<>();

            for (int i = 0; i < words.length; i++) {
                // Clean the word by removing punctuation and converting to lowercase.
                String currentWord = words[i].replaceAll("[^a-zA-Z]", "").toLowerCase();
                // Process only if the current word is in the frequent words set.
                if (!frequentWords.contains(currentWord))
                    continue;

                // Determine the window boundaries.
                int start = Math.max(0, i - windowDistance);
                int end = Math.min(words.length - 1, i + windowDistance);

                // For each neighbor in the window (skipping the current word).
                for (int j = start; j <= end; j++) {
                    if (j == i)
                        continue;

                    String neighborWord = words[j].replaceAll("[^a-zA-Z]", "").toLowerCase();
                    if (!frequentWords.contains(neighborWord))
                        continue;

                    // Create the pair key (format: "word,neighbor").
                    String pairKey = currentWord + "," + neighborWord;

                    if (aggregationLevel == 1) {
                        // Function-level aggregation: aggregate within the current line.
                        lineLocalCounts.put(pairKey, lineLocalCounts.getOrDefault(pairKey, 0) + 1);
                    } else {
                        // Class-level aggregation: aggregate across all lines.
                        localCounts.put(pairKey, localCounts.getOrDefault(pairKey, 0) + 1);
                    }
                }
            }

            if (aggregationLevel == 1) {
                // Emit function-level aggregated counts.
                for (Map.Entry<String, Integer> entry : lineLocalCounts.entrySet()) {
                    pair.set(entry.getKey());
                    context.write(pair, new IntWritable(entry.getValue()));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (aggregationLevel == 2) {
                // Emit class-level aggregated counts.
                for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                    pair.set(entry.getKey());
                    context.write(pair, new IntWritable(entry.getValue()));
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
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                    "Usage: PairsAggregation <input> <output> <frequentWordsFile> <windowDistance> <aggregationLevel: 1|2>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        int windowDistance = Integer.parseInt(args[3]);
        int aggregationLevel = Integer.parseInt(args[4]);
        conf.setInt("windowDistance", windowDistance);
        conf.setInt("aggregationLevel", aggregationLevel);

        Job job = Job.getInstance(conf, "Co-occurrence pairs with Aggregation");
        job.setJarByClass(PairsAggregation.class);
        job.setMapperClass(PairsMapper.class);
        job.setReducerClass(PairsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new URI(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}