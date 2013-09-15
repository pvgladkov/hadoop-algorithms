package org.apache.hadoop;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class VecNorm {

    /**
     * reducer
     */
    public static class VectorMapper extends Mapper<Object, Text, CustomKey, CustomValue>{


	    /**
	     *
	     * @param key
	     * @param value
	     * @param context
	     */
	    @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{

            StringTokenizer itr = new StringTokenizer(value.toString());

            int vectorId = Integer.parseInt(itr.nextToken());
            String vec = itr.nextToken();

            String[] vals = vec.split(":");

            int dimension = 0;
            for (String v:vals){
                int dimensionValue = Integer.parseInt(v);
                CustomValue CV = new CustomValue(vectorId, dimensionValue);
                context.write(new CustomKey(dimension, 0), CV);
                context.write(new CustomKey(dimension, 1), CV);
                dimension++;
            }
	    }
    }

    public static class CustomValue implements WritableComparable<CustomValue>{

        private int vectorId;
        private int dimensionValue;

        public CustomValue(){}

        public CustomValue(int vectorId, int dimensionValue) {
            this.vectorId = vectorId;
            this.dimensionValue = dimensionValue;
        }

        public int getVectorId() {
            return vectorId;
        }

        public int getDimensionValue() {
            return dimensionValue;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException{
            dataOutput.writeInt(vectorId);
            dataOutput.writeInt(dimensionValue);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException{
            vectorId = dataInput.readInt();
            dimensionValue = dataInput.readInt();
        }

        @Override
        public int compareTo(CustomValue value){
            return Integer.compare(this.dimensionValue, value.getDimensionValue());
        }
    }

    public static class CustomKey implements WritableComparable<CustomKey>{

        private int dimension;
        private int flag;

        public CustomKey(){}

        public CustomKey(int d, int f){
            dimension = d;
            flag = f;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException{
            dataOutput.writeInt(dimension);
            dataOutput.writeInt(flag);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException{
            dimension = dataInput.readInt();
            flag = dataInput.readInt();
        }

        @Override
        public int compareTo(CustomKey key){

            int returnVal = Integer.compare(this.dimension, key.getDimension());
            if(returnVal != 0){
                return returnVal;
            }
            if(this.flag == 0){
                return -1;
            }else if(key.getFlag() == 0){
                return 1;
            }

            return Integer.compare(this.flag, key.getFlag());
        }

        public int getFlag(){
            return flag;
        }

        public int getDimension(){
            return dimension;
        }
    }

    public static class VectorPartitioner extends Partitioner<CustomKey, CustomValue>{

        @Override
        public int getPartition(CustomKey key, CustomValue value, int num){
            return key.getDimension() % num;
        }
    }

    /**
     *
     */
    public static class VectorReducer extends Reducer<CustomKey,CustomValue,Text,Text> {

        private int m;
        private int M;

        private boolean isFirst;

        private int p;

        @Override
        public void setup(Context context){
            p = -1;
        }

	    /**
	     *
	     * @param key
	     * @param values
	     * @param context
	     */
        @Override
	    public void reduce(CustomKey key, Iterable<CustomValue> values, Context context)
                throws InterruptedException, IOException{

            if (p != key.getDimension()){
                isFirst = true;
                p = key.getDimension();
            }

            if (key.getFlag() == 0){
                for (CustomValue cv:values){
                    if (isFirst){
                        m = cv.getDimensionValue();
                        M = cv.getDimensionValue();
                        isFirst = false;
                    }
                    if (m > cv.getDimensionValue()){
                        m = cv.getDimensionValue();
                    }
                    if (M < cv.getDimensionValue()){
                        M = cv.getDimensionValue();
                    }
                }
            }else {
                for (CustomValue cv:values){
                    double newValue;
                    if (m == M){
                       newValue = 1;
                    }else {
                        newValue = (float)(cv.getDimensionValue() - m)/(M - m);
                    }

                    String str = Integer.toString(key.getDimension()) + ':' + Double.toString(newValue);
                    Text vec = new Text(str);

                    context.write(new Text(Integer.toString(cv.getVectorId())), vec);
                }
            }
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(CustomValue.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
