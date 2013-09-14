package org.apache.hadoop;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created with IntelliJ IDEA.
 * User: pavel
 * Date: 09.09.13
 * Time: 22:10
 * To change this template use File | Settings | File Templates.
 */
public class VecNorm {

    /**
     * reducer
     */
    public static class VectorMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

	    /**
	     *
	     * @param key
	     * @param value
	     * @param context
	     */
	    public void map(Object key, Text value, Context context){

	    }
    }

    public static class CustomKey implements WritableComparable<CustomKey>{

        private int key1;
        private int flag;

        @Override
        public void write(DataOutput dataOutput) throws IOException{
            dataOutput.write(key1);
            dataOutput.write(flag);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException{
            this.key1 = dataInput.readInt();
            this.flag = dataInput.readInt();
        }

        @Override
        public int compareTo(CustomKey key){
            return Integer.compare(this.flag, key.getFlag());
        }

        public int getFlag(){
            return this.flag;
        }

        public int getKey1(){
            return this.key1;
        }
    }

    public static class VectorPartitioner extends Partitioner<CustomKey, Text>{

        @Override
        public int getPartition(CustomKey key, Text value, int num){
            return key.getKey1() % num;
        }
    }

    /**
     *
     */
    public static class VectorReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	    /**
	     *
	     * @param key
	     * @param values
	     * @param context
	     */
	    public void reduce(Text key, Iterable<IntWritable> values, Context context){

	    }

    }

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: norm <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "norm");
        job.setReducerClass(VectorReducer.class);
        job.setMapperClass(VectorMapper.class);
        job.setPartitionerClass(VectorPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
