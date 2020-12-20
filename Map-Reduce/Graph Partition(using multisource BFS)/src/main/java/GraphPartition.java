import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    public long size;

   public Vertex () {
	id=0;
	adjacent=new Vector<Long>();
	centroid=0;
	depth=0;}

    Vertex(long id, Vector<Long>adjacent,long centroid,short depth){
        this.id = id;
        this.adjacent = adjacent;
        this.centroid = centroid;
        this.depth = depth;
	this.size = adjacent.size();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
	dataOutput.writeLong(centroid);
	dataOutput.writeShort(depth);
	dataOutput.writeLong(size);
        for(int i=0;i<adjacent.size();i++){  /*TODO Changes in the for loop*/
            dataOutput.writeLong(adjacent.get(i));
        }        
	}
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
	centroid = dataInput.readLong();
	depth = dataInput.readShort();
	adjacent = new Vector<Long>();
	size = dataInput.readLong();
        for(int j=0;j<size;j++){ /*TODO changes in the for loop*/
           adjacent.add(dataInput.readLong());
        }
    }
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static int count=0;

    /* ... */
    public static class MapperFirst extends Mapper<Object, Text, LongWritable, Vertex>{
        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
	long centroid=-1;   
	Scanner s = new Scanner(value.toString()).useDelimiter(",");           
        long id = s.nextLong();
	Vector<Long> adjacent = new Vector<>();
	while(s.hasNextLong()){
		long vrts = s.nextLong();
		adjacent.add(vrts);
	}                        
	if (count<10){
		centroid=id;
		centroids.add(id);
		count++;
		context.write(new LongWritable(id),new Vertex(id,adjacent,centroid,(short)0));}
	else {context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,(short)0));}
  }
 }


    public static class MapperSecond extends Mapper<LongWritable, Vertex, LongWritable, Vertex>{
        @Override
        public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException{
            context.write(new LongWritable(vertex.id), vertex);
            Vector<Long> temp = new Vector<>();
            if (vertex.centroid>0){
                for(Long n: vertex.adjacent){
                    context.write(new LongWritable(n), new Vertex(n,temp,vertex.centroid,BFS_depth));
            }
        }
    }
}

    public static class ReducerSecond extends Reducer<LongWritable, Vertex, LongWritable, Vertex>{
        @Override
        public void reduce(LongWritable vid, Iterable<Vertex> values, Context context) throws IOException, InterruptedException{
            short min_depth = 1000;
            Vector<Long> temp = new Vector<>();
            long nvid = vid.get();
	    Vertex m = new Vertex(nvid, temp, (long)-1, (short) 0);
            for(Vertex v:values){
                if(!(v.adjacent.isEmpty())){
                    m.adjacent=v.adjacent;
                }
                if(v.centroid>0 && v.depth<min_depth){
                    min_depth=v.depth;
                    m.centroid=v.centroid;
                }
            }
            m.depth = min_depth;
            context.write(new LongWritable(nvid),m);
        }
    }
    public static class MapperThird extends Mapper<LongWritable, Vertex, LongWritable, LongWritable>{
        @Override
        public void map(LongWritable centroid, Vertex value, Context context) throws IOException, InterruptedException{
            context.write(new LongWritable(value.centroid),new LongWritable(1));
        }
    }
    public static class ReducerThird extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
        @Override
        public void reduce(LongWritable centroid, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
            long m=0;
            for(LongWritable v:values){
                m = m + (long)v.get(); 
          }
            context.write(centroid,new LongWritable(m));
        }
    }
    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob1");
        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(GraphPartition.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
	job.setMapperClass(MapperFirst.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);       
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path( args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job.setJobName("MyJob2");
            /* ... First Map-Reduce job to read the graph */
            job.setJarByClass(GraphPartition.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setMapperClass(MapperSecond.class);
            job.setReducerClass(ReducerSecond.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+"/i"+i));
	    FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job.setJobName("MyJob3");
        job.setJarByClass(GraphPartition.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(MapperThird.class);
        job.setReducerClass(ReducerThird.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[1]+"/i8"));
	FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
