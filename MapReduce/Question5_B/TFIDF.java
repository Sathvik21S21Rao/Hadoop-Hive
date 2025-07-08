package org.nosql;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import opennlp.tools.stemmer.PorterStemmer;

public class TFIDF {
    public static class CustomMapper extends Mapper<Object, Text, Text, MapWritable> {
        private MapWritable top100 = new MapWritable();
        PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Configuration conf = context.getConfiguration();
//            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                String line = "";

                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    String[] tokens = line.split("\t");
                    top100.put(new Text(tokens[0]), new IntWritable(0));
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Text filename = new Text(fileSplit.getPath().getName().replace(".txt", ""));

            MapWritable wordCountMap = new MapWritable();

            String line = value.toString();
            String[] tokens = line.split("\\s+");

            for (String token : tokens) {
                token = stemmer.stem(token);
                Text tokenText = new Text(token);
                if (top100.containsKey(tokenText)) {
                    int count = wordCountMap.containsKey(tokenText) ? ((IntWritable) wordCountMap.get(tokenText)).get() : 0;
                    wordCountMap.put(tokenText, new IntWritable(count + 1));
                }
            }

            context.write(filename, wordCountMap);
        }
    }

    private static double computeTFIDF(int tf, int df, int totalDoc) {
        return tf* Math.log(totalDoc / (df+1));
    }

    public static class CustomReducer extends Reducer<Text, MapWritable, Text, Text> {
        private Text key1 = new Text();
        private Text value1 = new Text();
        private Map<String, Integer> top100 = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Configuration conf = context.getConfiguration();

            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                String line = "";
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    String[] tokens = line.split("\t");
                    top100.put(tokens[0], Integer.parseInt(tokens[1]));
                }
                reader.close();
            }
        }
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            Map<String, Double> result = new HashMap<String, Double>();
            for (MapWritable value : values) {
                for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                    String word=((Text)entry.getKey()).toString();
                    int tf=((IntWritable)entry.getValue()).get();
                    int df=top100.getOrDefault(word,0);
                    double tfidf = computeTFIDF(tf, df, 10000);
                    if(result.containsKey(word)){
                        result.put(word,result.get(word)+tfidf);
                    }
                    else
                        result.put(word,tfidf);
                }
            }
            for (Map.Entry<String, Double> entry : result.entrySet()) {
                key1.set(key.toString());
                value1.set(entry.getKey() + "\t" + entry.getValue());
                context.write(key1, value1);
            }


        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TFIDF <input path> <output path> <cache file URI>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
