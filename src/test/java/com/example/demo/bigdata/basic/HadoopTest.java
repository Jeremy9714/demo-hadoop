package com.example.demo.bigdata.basic;

import com.example.demo.bigdata.console.Application;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Chenyang
 * @create 2024-10-15-16:12
 */
@SpringBootTest(classes = Application.class)
@RunWith(SpringRunner.class)
public class HadoopTest {
    @Test
    public void test1() {
//        FileSystem.get();
    }
}
