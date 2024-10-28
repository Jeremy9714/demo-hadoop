package com.example.demo.bigdata.tutorial.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/26 20:53
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    
    private String userId;
    private String ipAddress;
    private String eventType;
    private Long timestamp;
}
