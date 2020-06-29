import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFS_Test {
    /**
     * miles
     * @throws IOException
     */
    @Test
    public void initHDFS() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(fileSystem);
    }

    /**
     * false 是否删除源文件
     * 目标文件地址
     * 下载目的地
     * 是否校验文件
     * @throws Exception
     */
    @Test
    public void putHDFS() throws Exception {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://bigdata111:9000");
        conf.set("dfs.replication", "4");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf,"root");
//        System.out.println(fileSystem);
        fileSystem.copyToLocalFile(false, new Path("/yarn-site.xml"), new Path("/Users/bocai/Downloads/tmp/a.xml"), true);
        fileSystem.close();
        System.out.println("下载成功");
    }

    @Test
    public void mkdirHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        fileSystem.mkdirs(new Path("/A/B/C/D"));
        fileSystem.close();
        System.out.println("创建成功");
    }


    @Test
    public void deleteHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        fileSystem.delete(new Path("/A/B/C/D"), false);
        fileSystem.close();
        System.out.println("删除成功");
    }


    @Test
    public void renameHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        fileSystem.rename(new Path("/A.txt"), new Path("/B.txt"));
        fileSystem.close();
        System.out.println("改名成功");
    }

    @Test
    public void fileInfoHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("文件名称: " + fileStatus.getPath().getName());
            System.out.println("文件块大小: " + fileStatus.getBlockSize());
            System.out.println("文件权限: " + fileStatus.getPermission());
        }
    }

    @Test
    public void judgeHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/A"));
        for(FileStatus status : listStatus){
           if(status.isFile()){
               System.out.println("这是个文件: " + status.getPath().getName());
           }else{
               System.out.println("这是个目录: " + status.getPath().getName());
           }
        }
    }

    @Test
    public void putFileToHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //create file input stream
        FileInputStream fsi = new FileInputStream(new File("/Users/bocai/Downloads/example-gui-config.json"));
        // create file output stream
        FSDataOutputStream fso = fileSystem.create(new Path("/example-gui-config.json"));
        IOUtils.copyBytes(fsi, fso, 4 * 1024, false);
        IOUtils.closeStream(fsi);
        IOUtils.closeStream(fso);
        System.out.println("上传成功");
    }
}
