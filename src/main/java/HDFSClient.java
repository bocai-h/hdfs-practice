import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author bocai
 */
public class HDFSClient {
    public static void main(String[] args) throws IOException {
         // 获取文件系统
        Configuration conf = new Configuration();
        //连接集群
        conf.set("fs.defaultFS", "hdfs://bigdata111:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        //把本地文件上传到hdfs
        fileSystem.copyFromLocalFile(new Path("/Users/bocai/Downloads/tmp/test.txt"), new Path("/test.txt"));
        //关闭资源
        fileSystem.close();
        System.out.printf("over");
    }
}
