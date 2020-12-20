import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Graph {
    public static class MapperFirst extends Mapper<Object,Text,LongWritable,LongWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long key2 = s.nextLong();
            long value2 = s.nextLong();
            context.write(new LongWritable(key2),new LongWritable(value2));
            s.close();
        }
    }
    public static class ReducerFirst extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> nodes, Context context )
                throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable n: nodes) {
                count++;
	    }
          
            context.write(key,new LongWritable(count));
        }
    }
    public static class MapperSecond extends Mapper<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void map ( LongWritable node, LongWritable count, Context context )
                throws IOException, InterruptedException {
           /* context.write(count, 1);*/
	    context.write(count, new LongWritable(1));
        }
    }
    public static class ReducerSecond extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v: values) {
                sum+=v.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }
    public static void main ( String[] args ) throws Exception {
        /*Configuration conf = getConf();*/
	Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setMapperClass(MapperFirst.class);
        job1.setReducerClass(ReducerFirst.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path("temporary_output"));
//        return job.waitForCompletion(true)? 0:1;
	job1.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapperClass(MapperSecond.class);
        job2.setReducerClass(ReducerSecond.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path("temporary_output"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
        
    }

}




