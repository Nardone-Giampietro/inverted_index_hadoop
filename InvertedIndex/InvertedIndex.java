package it.unipi.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

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
    
        Map<Word_File, Integer> H = new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {   
            
            String file = ((FileSplit) context.getInputSplit()).getPath().getName();

            String line = value.toString();
            String[] words = line.split("[\\s\\p{Punct}]+"); 
            for(String word: words){
                if (word != null && !word.trim().isEmpty()) {
                    String lowerWord = word.toLowerCase();
                    Word_File wf = new Word_File(lowerWord, file);
                    H.merge(wf, 1, Integer::sum);
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            for(Map.Entry<Word_File, Integer> entry: H.entrySet()){
                Word_File wf = entry.getKey();
                int val = entry.getValue();
                context.write(new Text(wf.word), new File_Value(wf.file, val));
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, File_Value, Text, Text>
    {

        @Override
        public void reduce(Text key, Iterable<File_Value> values, Context context) throws IOException, InterruptedException 
        {
            Map<String, Integer> H = new HashMap<>();

            for (File_Value fv : values) {
                H.merge(fv.getFile(), fv.getValue(), Integer::sum);
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
        if (otherArgs.length != 2) {
           System.err.println("Usage: InvertedIndex <input> <output>");
           System.exit(1);
        }
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        Job job = Job.getInstance(conf, "InvertedIndex");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(File_Value.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(12);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}

