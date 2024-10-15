package com.example.demo.bigdata.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

/**
 * @author Chenyang
 * @create 2024-10-15-15:49
 */
@SpringBootApplication
@ServletComponentScan(basePackages = {"com.example.demo.bigdata.*"})
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
