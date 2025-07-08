package org.nosql;

import java.io.*;
import java.util.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import opennlp.tools.stemmer.PorterStemmer;

public class Main
{

    public static class DFMapper extends Mapper<Object, Text, Text, Text>
    {
        PorterStemmer stemmer = new PorterStemmer();
        Set<String> stopwords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("org/nosql/stopwords.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                stopwords.add(line.trim().toLowerCase());
            }
            reader.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String documentId = key.toString();
            Text docId = new Text();
            docId.set(documentId);
            Set<String> uniqueWords = new HashSet<>();

            for (String token : value.toString().toLowerCase().split("\\s+"))
            {
                token = token.replaceAll("[^a-zA-Z0-9]", "");
                String stemmed = stemmer.stem(token);
                if (!stemmed.isEmpty() && !stopwords.contains(stemmed))
                {
                    uniqueWords.add(stemmed);
                }
            }

            for (String term : uniqueWords)
            {
                Text word = new Text();
                word.set(term);
                context.write(word, docId);
            }
        }
    }

    public static class DFReducer extends Reducer<Text, Text, Text, IntWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (Text docId : values) {
                sum += 1;
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Document Frequency");
        job.setJarByClass(Main.class);
        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
