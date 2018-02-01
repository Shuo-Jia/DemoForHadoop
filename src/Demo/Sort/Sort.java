package Demo.Sort;

import Demo.Dedup.Dedup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Sort {
  public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
    private static IntWritable data = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      data.set(Integer.parseInt(line));
      context.write(data, new IntWritable(1));
      System.out.println("MAP输出："+Integer.parseInt(line)+"  "+1);
    }
  }

  public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static IntWritable linenum = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      for (IntWritable val : values) {
        context.write(linenum, key);
        linenum = new IntWritable(linenum.get() + 1);
      }
    }
  }

  public static class Partition extends Partitioner<IntWritable, IntWritable> {

    @Override
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
      System.out.println("Partition输入："+key+" "+value+""+numPartitions);
      int Maxnumbers = 65223;
      int bound = Maxnumbers / numPartitions + 1;
      int keynumbers = key.get();
      for (int i = 0; i < numPartitions; i++) {
        if(keynumbers < bound*i && keynumbers >= bound*(i-1))
          return i-1;
      }
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.out.println("Usage: Sort <in> <out>");
      System.exit(2);
    }

    Path path = new Path(args[1]);
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path,true);
      System.out.println("This path exited have delete!");
    }

    Job job = Job.getInstance(conf, "Data Sort");
    job.setJarByClass(Sort.class);
    job.setMapperClass(Map.class);
    job.setPartitionerClass(Partition.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
