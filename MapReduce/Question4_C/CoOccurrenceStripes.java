import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
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

public class CoOccurrenceStripes {

    public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        private Set<String> frequentWords = new HashSet<>();
        private int windowDistance = 1;
        private Text currentWordText = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get window distance parameter
            windowDistance = context.getConfiguration().getInt("windowDistance", 1);
            
            // Load the frequent words file from the distributed cache.
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    // Open the file using its file name
                    try (BufferedReader reader = new BufferedReader(
                            new FileReader(new File(new Path(cacheFile.getPath()).getName())))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            // Extract only the first token (word) from the line.
                            String[] tokens = line.split("\\s+");
                            if (tokens.length > 0) {
                                frequentWords.add(tokens[0].toLowerCase());
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            // Split the line into words
            String[] words = value.toString().split("\\s+");
            for (int i = 0; i < words.length; i++) {
                // Clean the word and check if it's frequent
                String currentWord = words[i].replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!frequentWords.contains(currentWord)) continue;
                currentWordText.set(currentWord);
                
                // Create a stripe (map) for the current word.
                MapWritable stripe = new MapWritable();
                
                // Define window boundaries
                int start = Math.max(0, i - windowDistance);
                int end = Math.min(words.length - 1, i + windowDistance);
                
                for (int j = start; j <= end; j++) {
                    if (j == i) continue; // skip the current word itself
                    String neighborWord = words[j].replaceAll("[^a-zA-Z]", "").toLowerCase();
                    if (!frequentWords.contains(neighborWord)) continue;
                    
                    Text neighborKey = new Text(neighborWord);
                    // Increase count in the stripe
                    if (stripe.containsKey(neighborKey)) {
                        IntWritable count = (IntWritable) stripe.get(neighborKey);
                        count.set(count.get() + 1);
                        stripe.put(neighborKey, count);
                    } else {
                        stripe.put(neighborKey, new IntWritable(1));
                    }
                }
                
                // Emit only if stripe is not empty.
                if (!stripe.isEmpty()) {
                    context.write(currentWordText, stripe);
                }
            }
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) 
                throws IOException, InterruptedException {
            MapWritable combinedStripe = new MapWritable();
            
            // Merge all stripes for the key.
            for (MapWritable stripe : values) {
                for (Writable neighbor : stripe.keySet()) {
                    IntWritable count = (IntWritable) stripe.get(neighbor);
                    if (combinedStripe.containsKey(neighbor)) {
                        IntWritable sum = (IntWritable) combinedStripe.get(neighbor);
                        sum.set(sum.get() + count.get());
                        combinedStripe.put(neighbor, sum);
                    } else {
                        combinedStripe.put(neighbor, new IntWritable(count.get()));
                    }
                }
            }
            context.write(key, combinedStripe);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: CoOccurrenceStripes <input> <output> <frequentWordsFile> [windowDistance]");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        int windowDistance = 1;
        if (args.length == 4) {
            windowDistance = Integer.parseInt(args[3]);
        }
        conf.setInt("windowDistance", windowDistance);

        Job job = Job.getInstance(conf, "Co-occurrence stripes");
        job.setJarByClass(CoOccurrenceStripes.class);
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
