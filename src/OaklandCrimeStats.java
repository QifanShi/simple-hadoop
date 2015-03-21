import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class counts the total number of crime occurred within 2000 feet of 3803 Forbes Ave.
 * @author Qifan Shi
 * @version 1.0 Last Modified: 11/14/2014
 */
public class OaklandCrimeStats extends Configured implements Tool {

        public static class OaklandCrimeStatsMap extends Mapper<LongWritable, Text, Text, IntWritable>
        {
                private final static IntWritable one = new IntWritable(1);
                private Text word = new Text("Oakland Crime");
                
                @Override
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        
                        String line = value.toString();
                        
                        StringTokenizer tokenizer = new StringTokenizer(line);
                        if(tokenizer.hasMoreTokens()){
                            // split the whole record by tab
                            String[] token = line.split("\t");
                            if(!token[0].equals("X")){
                                // the first and second token is needed for computing distance
                                double x = Double.parseDouble(token[0]);
                                double y = Double.parseDouble(token[1]);
                            
                                // coordinates of 3803 Forbes Ave.  
                                double x2 = 1354326.897;
                                double y2 = 411447.7828;
                                // compute distance
                                double dist = Math.sqrt(Math.pow((x - x2), 2) + Math.pow((y - y2), 2));
                                if(dist <= 2000)
                                    context.write(word, one);
                            }
                        }
                }
        }
        
        public static class OaklandCrimeStatsReducer extends Reducer<Text, IntWritable, Text, IntWritable>
        {
                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
                {
                        int sum = 0;
                        for(IntWritable value: values)
                        {
                                sum += value.get();
                        }
                        context.write(key, new IntWritable(sum));
                }
                
        }
        
        public int run(String[] args) throws Exception  {
               
                Job job = new Job(getConf());
                job.setJarByClass(OaklandCrimeStats.class);
                job.setJobName("Oakland crime");
                
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                
                job.setMapperClass(OaklandCrimeStatsMap.class);
                job.setCombinerClass(OaklandCrimeStatsReducer.class);
                job.setReducerClass(OaklandCrimeStatsReducer.class);
                
                
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                
                
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                
                boolean success = job.waitForCompletion(true);
                return success ? 0: 1;
        }
        
       
        public static void main(String[] args) throws Exception {
                int result = ToolRunner.run(new OaklandCrimeStats(), args);
                System.exit(result);
        }
       
} 

