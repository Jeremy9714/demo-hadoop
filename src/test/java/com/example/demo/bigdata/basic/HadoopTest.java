package com.example.demo.bigdata.basic;

import com.example.demo.bigdata.console.Application;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * @author Chenyang
 * @create 2024-10-15-16:12
 */
@Slf4j
@SpringBootTest(classes = Application.class)
//@RunWith(SpringRunner.class)
public class HadoopTest {

    private FileSystem fs;

    @BeforeEach
    public void init() {
        System.out.println("---------beforeEach--------");
        try {
            // namenode地址
            URI uri = new URI("hdfs://hadoop202:8020");
            // 配置
            Configuration conf = new Configuration();
            conf.set("dfs.replication", "2"); // 副本数量
            // 用户
            String user = "jeremy";
            fs = FileSystem.get(uri, conf, user);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @After
    public void destroy() {
        System.out.println("----------after----------");
        if (fs != null) {
            try {
                fs.close();
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testMkDir() {
        try {
            fs.mkdirs(new Path("/newdir"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * copyFromLocal
     */
    @Test
    public void testUpload() {
        try {
            fs.copyFromLocalFile(false, true, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\wordCount.txt"),
                    new Path("hdfs://hadoop202/newdir"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * copyToLocal
     */
    @Test
    public void testDownLoad() {
        try {
            fs.copyToLocalFile(false, new Path("hdfs://hadoop202/newdir/wordCount.txt"),
                    new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\newWordCount.txt"), false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * delete
     */
    @Test
    public void testDelete() {
        try {
            // 删除文件
            fs.delete(new Path("/newdir/wordCount.txt"), false);
            // 删除空目录
            fs.delete(new Path("/newdir"), false);
            // 删除目录
            fs.delete(new Path("/wcoutput"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * rename
     */
    @Test
    public void testRename() {
        try {
            // 移动、重命名文件、目录
            fs.rename(new Path("/newdir/wordCount.txt"), new Path("/newdir/newWordCount.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * listLocatedStatus
     */
    @Test
    public void testListStatus() {
        try {
            RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(new Path("/"));
            while (iterator.hasNext()) {
                LocatedFileStatus status = iterator.next();
                System.out.println("======" + status.getPath() + "======");
                System.out.println(status.getPermission());
                System.out.println(status.getOwner());
                System.out.println(status.getGroup());
                System.out.println(status.getModificationTime());
                System.out.println(status.getPath().getName());
                if (status.isFile()) {
                    System.out.println(status.getLen());
                    System.out.println(status.getBlockSize());
                    System.out.println(status.getReplication());
                    BlockLocation[] blockLocations = status.getBlockLocations();
                    System.out.println(Arrays.toString(blockLocations));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
