package Demo.MTjoin;

import Demo.STjoin.STjoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MTjoin {

  public static int reduceTime = 0;

  public static class Map extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //map每次读取一行，所以当读取第一行的时候，忽略
      String line = value.toString();
      if (line.contains("factoryname") || line.contains("addressID"))
        return;

      String factoryname = new String();
      String addressID = new String();
      String addresname = new String();
      //如果是数字，代表是第二张表，将按照第二张表的字符串特点分割；否则按照第一张字符特点分割
      if (line.charAt(0) >= '0' && line.charAt(0) <= '9') {
        StringTokenizer itr = new StringTokenizer(line);
        int i = 0;
        String[] values = new String[2];
        while (itr.hasMoreTokens())
          values[i++] = itr.nextToken();
        addressID = values[0];
        addresname = values[1];
        context.write(new Text(addressID), new Text("0+" + addresname));
      } else {
        int i = 0;
        while (line.charAt(i) < '0' || line.charAt(i) > '9')
          i++;
        factoryname = line.substring(0, i - 2);
        addressID = line.substring(i);
        context.write(new Text(addressID), new Text("1+" + factoryname));
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      if (reduceTime++ == 0)
        context.write(new Text("factoryName"), new Text("addressName"));

      ArrayList<String> factorynames = new ArrayList<>();
      ArrayList<String> addresnames = new ArrayList<>();
      Iterator iterator = values.iterator();
      while (iterator.hasNext()) {
        String value = iterator.next().toString();
        if (value.charAt(0) == '0')
          addresnames.add(value.substring(2));
        else
          factorynames.add(value.substring(2));
      }
      if (!factorynames.isEmpty() && !addresnames.isEmpty()) {
        for (String factoryname : factorynames) {
          for (String addresname : addresnames)
            context.write(new Text(factoryname), new Text(addresname));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: Muti Table Join <in> <out>");
      System.exit(2);
    }

    Path path = new Path(args[1]);
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path, true);
      System.out.println("This path exited have delete!");
    }

    Job job = Job.getInstance(conf, "Multi Table Join");
    job.setJarByClass(MTjoin.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
