package it.unipi.project;

import java.io.IOException;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndex
{
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, File_Value> 
    {
        List<String> queryWords = new ArrayList<>();

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String query = conf.get("query");

            queryWords = Arrays.asList(query.toLowerCase().split("\\s+"));
        }
    
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String file = ((FileSplit) context.getInputSplit()).getPath().getName();

            String line = value.toString();
            String[] words = line.split("[\\s\\p{Punct}]+");

            for (String word : words) {
                if (word != null && !word.trim().isEmpty()) {
                    String lowerWord = word.toLowerCase();
                    if (queryWords.contains(lowerWord)) {
                        context.write(new Text(lowerWord), new File_Value(file, 1));
                    }
                }
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, File_Value, Text, File_Value> {

        @Override
        public void reduce(Text key, Iterable<File_Value> values, Context context)
                throws IOException, InterruptedException {
            
            Map<String, Integer> fileCounts = new HashMap<>();

            for (File_Value fv : values) {
                String file = fv.file;
                int count = fv.value;

                fileCounts.put(file, fileCounts.getOrDefault(file, 0) + count);
            }

            for (Map.Entry<String, Integer> entry : fileCounts.entrySet()) {
                context.write(key, new File_Value(entry.getKey(), entry.getValue()));
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, File_Value, Text, Text>
    {

        @Override
        public void reduce(Text key, Iterable<File_Value> values, Context context) throws IOException, InterruptedException 
        {
            Map<String, Integer> H = new HashMap<>();

            for(File_Value fv: values){
                if(H.containsKey(fv.file))
                    H.put(fv.file, H.get(fv.file)+fv.value);
                else
                    H.put(fv.file,fv.value);
            }

            String outputValue = "";
            
            for(Map.Entry<String, Integer> entry: H.entrySet()){
                String file = entry.getKey();
                int value = entry.getValue();
                outputValue = outputValue + "   " + file + ":" + value;
            }
            context.write(key, new Text(outputValue));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
           System.err.println("Usage: InvertedIndex <input1> [<input2> ...] <output> <query>");
           System.exit(1);
        }
        String query = otherArgs[otherArgs.length - 1];
        conf.set("query", query);

        Job job = Job.getInstance(conf, "InvertedIndex");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(File_Value.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}
