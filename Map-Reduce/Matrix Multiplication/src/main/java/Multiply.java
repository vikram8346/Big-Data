import java.io.*;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



class Elem implements Writable {
    public short tag;
    public int index;
    public double value;

    Elem() {} /*Constructor may be removed if not used*/

    Elem(short t, int i, double v) {
        tag = t;
        index = i;
        value = v;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeShort(tag);
        dataOutput.writeInt(index);
        dataOutput.writeDouble(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = dataInput.readShort();
        index = dataInput.readInt();
        value = dataInput.readDouble();
    }

}

class Pair implements WritableComparable<Pair>{
    int i;
    int j;

    Pair () {} /*Constructor may be removed if not used*/

    Pair(int pi, int pj){
        i=pi;
        j=pj;
    }

    @Override
    public int compareTo(Pair o) {
        return (i == o.i)? j-o.j : i-o.i; /*Given in bigdata103 slides, may use different one*/
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(i);
        dataOutput.writeInt(j);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        i=dataInput.readInt();
        j=dataInput.readInt();

    }
    public String toString() { return i+" "+j;}
}

public class Multiply {
    public static class Mat_M_Mapper extends Mapper<Object,Text, IntWritable, Elem>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
          Scanner s = new Scanner(value.toString()).useDelimiter(",");
          int inti = s.nextInt();
          int intj = s.nextInt();
          double dvalv = s.nextDouble();
          /*TODO*/
          //Elem e = new Elem((short) 0, s.nextInt(), s.nextDouble());
          //context.write(new IntWritable(s.nextInt()), e);
          Elem e = new Elem((short) 0, inti, dvalv);
          context.write(new IntWritable(intj), e); /*TODO Changes*/
          s.close();
        }
    }

    public static class Mat_N_Mapper extends Mapper<Object, Text, IntWritable, Elem>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int inti = s.nextInt();
            int intj = s.nextInt();
            double dvalv = s.nextDouble();
            /*TODO*/
            Elem e = new Elem((short) 1, intj, dvalv);
            context.write(new IntWritable(inti), e);
            s.close();
        }
    }

    public static class ReducerFirst extends Reducer<IntWritable, Elem, Pair, DoubleWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<Elem> values, Context context) throws IOException, InterruptedException{
            Vector<Elem> A = new Vector<>();
            Vector<Elem> B = new Vector<>();
            for(Elem v: values) {
                if(v.tag == 0)
                    A.add(new Elem((short) 0, v.index, v.value)); /*TODO Try A.add(a.Clone())*/
                else B.add(new Elem((short) 1, v.index, v.value));
            }
            for (Elem a: A) {
                for (Elem b: B) {
                    context.write(new Pair(a.index,b.index), new DoubleWritable(a.value*b.value));
                }
            }
        }
    }                                                                                                                

    public static class MapperSecond extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable>{
        @Override
        public void map(Pair key, DoubleWritable value, Context context) throws IOException, InterruptedException{
	    //Scanner s = new Scanner(value.toString());
	    //int inti = s.nextInt(); /*TODO Try SequenceFileInputFormat to avoid parsing all the values every time*/
            //int intj = s.nextInt();
            //double dvalv = s.nextDouble();
            //Pair mp = new Pair(inti,intj);
	    context.write(key,value);

//}
        }
    }

    public static class ReducerSecond extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable>{
        @Override
        public void reduce(Pair pair, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            double m = 0;
            for(DoubleWritable v: values) {
                m += v.get();
            }
            context.write(pair, new DoubleWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setReducerClass(ReducerFirst.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class); /*TODO Try SequenceFileOutputFormat instead of TextFileInputFormat*/
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,Mat_M_Mapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,Mat_N_Mapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(MapperSecond.class);
        job2.setReducerClass(ReducerSecond.class);
	job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[2]));
        //MultipleInputs.addInputPath(job2,new Path(args[2]),SequenceFileInputFormat.class);       
	FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
    }
}
