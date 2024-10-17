package com.example.demo.bigdata.tutorial.yarn.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * @author Chenyang
 * @create 2024-10-17 16:31
 * @description 支持 -D参数运行
 */
public class YarnWordCountDriver {

    private static Tool tool;

    public static void main(String[] args) throws Exception {

        // 创建配置
        Configuration conf = new Configuration();

        switch (args[0]) {
            case "wordCount":
            case "wordcount":
                tool = new YarnWordCount();
                break;
            default:
                throw new RuntimeException("No such method");
        }

        // 运行程序
        int result = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(result);
    }
}
