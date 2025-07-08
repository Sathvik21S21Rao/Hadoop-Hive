import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripesAggregation {

    public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        private Set<String> frequentWords = new HashSet<>();
        private int windowDistance = 1;
        private int aggregationLevel = 1;
        private Text currentWordText = new Text();
        private MapWritable classLevelStripe = new MapWritable(); // For class-level aggregation

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get window distance and aggregation level from configuration
            windowDistance = context.getConfiguration().getInt("windowDistance", 1);
            aggregationLevel = context.getConfiguration().getInt("aggregationLevel", 1);

            // Load the frequent words file from the distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    BufferedReader reader = new BufferedReader(
                            new FileReader(new File(new Path(cacheFile.getPath()).getName())));
                    try {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] tokens = line.split("\\s+");
                            if (tokens.length > 0) {
                                frequentWords.add(tokens[0].toLowerCase());
                            }
                        }
                    } finally {
                        reader.close();
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");
            MapWritable functionLevelStripe = new MapWritable(); // For function-level aggregation

            for (int i = 0; i < words.length; i++) {
                String currentWord = words[i].replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!frequentWords.contains(currentWord))
                    continue;
                currentWordText.set(currentWord);

                // Define window boundaries
                int start = Math.max(0, i - windowDistance);
                int end = Math.min(words.length - 1, i + windowDistance);

                for (int j = start; j <= end; j++) {
                    if (j == i)
                        continue; // Skip the current word itself
                    String neighborWord = words[j].replaceAll("[^a-zA-Z]", "").toLowerCase();
                    if (!frequentWords.contains(neighborWord))
                        continue;

                    Text neighborKey = new Text(neighborWord);

                    if (aggregationLevel == 1) {
                        // Function-level aggregation: aggregate within the current line
                        if (functionLevelStripe.containsKey(neighborKey)) {
                            IntWritable count = (IntWritable) functionLevelStripe.get(neighborKey);
                            count.set(count.get() + 1);
                        } else {
                            functionLevelStripe.put(neighborKey, new IntWritable(1));
                        }
                    } else {
                        // Class-level aggregation: aggregate across all lines
                        if (classLevelStripe.containsKey(neighborKey)) {
                            IntWritable count = (IntWritable) classLevelStripe.get(neighborKey);
                            count.set(count.get() + 1);
                        } else {
                            classLevelStripe.put(neighborKey, new IntWritable(1));
                        }
                    }
                }
            }

            if (aggregationLevel == 1 && !functionLevelStripe.isEmpty()) {
                // Emit function-level aggregated stripe
                context.write(currentWordText, functionLevelStripe);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (aggregationLevel == 2 && !classLevelStripe.isEmpty()) {
                // Emit class-level aggregated stripe
                context.write(currentWordText, classLevelStripe);
            }
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            MapWritable combinedStripe = new MapWritable();

            // Merge all stripes for the key
            for (MapWritable stripe : values) {
                for (Writable neighbor : stripe.keySet()) {
                    IntWritable count = (IntWritable) stripe.get(neighbor);
                    if (combinedStripe.containsKey(neighbor)) {
                        IntWritable sum = (IntWritable) combinedStripe.get(neighbor);
                        sum.set(sum.get() + count.get());
                    } else {
                        combinedStripe.put(neighbor, new IntWritable(count.get()));
                    }
                }
            }
            context.write(key, combinedStripe);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                    "Usage: StripesAggregation <input> <output> <frequentWordsFile> <windowDistance> <aggregationLevel: 1|2>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        int windowDistance = Integer.parseInt(args[3]);
        int aggregationLevel = Integer.parseInt(args[4]);
        conf.setInt("windowDistance", windowDistance);
        conf.setInt("aggregationLevel", aggregationLevel);

        Job job = Job.getInstance(conf, "Co-occurrence stripes with Aggregation");
        job.setJarByClass(StripesAggregation.class);
        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Add frequent words file to distributed cache
        job.addCacheFile(new URI(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}