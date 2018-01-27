import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;


import java.net.URI;

public class Test {
  public static String hdfsUrl="hdfs://192.168.68.128:9000";
  //获取hdfs的句柄
  public static FileSystem getHdfs()throws Exception{
    //获取配置文件信息
    Configuration conf=new Configuration();
    //获取文件系统
    FileSystem hdfs=FileSystem.get(URI.create(hdfsUrl),conf);
    return hdfs;
  }
  //读取文件信息
  public static void testRead(Path p) throws Exception{
    FileSystem hdfs=getHdfs();
    //打开文件流
    FSDataInputStream inStream=hdfs.open(p);
    //读取文件内容到控制台显示
    IOUtils.copyBytes(inStream, System.out,4096,false);
    //关闭文件流
    IOUtils.closeStream(inStream);
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("hadoop.home.dir", "F:\\Program Files\\hadoop-3.0.0");
    Path p = new Path("/user/root/hdfsOutput/part-r-00000");
    Test.testRead(p);
  }
}
