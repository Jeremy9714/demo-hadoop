package com.example.demo.bigdata.rpc;

import com.example.demo.bigdata.console.Application;
import com.example.demo.bigdata.tutorial.jdk.rpc.entity.RpcEntity;
import com.example.demo.bigdata.tutorial.jdk.rpc.service.RpcEntityService;
import com.example.demo.bigdata.tutorial.jdk.rpc.service.impl.RpcEntityServiceImpl;
import com.example.demo.bigdata.tutorial.jdk.rpc.util.CommonStubService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/18 17:16
 * @Version: 1.0
 */
@SpringBootTest(classes = Application.class)
public class RpcStubTest {

    @Test
    public void client() {
        CommonStubService<RpcEntityService> stubService = new CommonStubService<>(RpcEntityService.class);
        RpcEntityService rpcEntityService = stubService.getStub();
        RpcEntity entity = rpcEntityService.getEntityById("");
        System.out.println(entity);
    }

    @Test
    public void server() throws Exception {
        ServerSocket serverSocket = new ServerSocket(12345);
        System.out.println("===========服务端已启动==========");

        while (!serverSocket.isClosed()) {
            Socket socket = serverSocket.accept();
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

            String methodName = ois.readUTF();
            Class[] parameterTypes = (Class[]) ois.readObject();
            Object[] args = (Object[]) ois.readObject();

            RpcEntityServiceImpl rpcEntityService = new RpcEntityServiceImpl();
            Method method = rpcEntityService.getClass().getDeclaredMethod(methodName, parameterTypes);
            RpcEntity entity = (RpcEntity) method.invoke(rpcEntityService, args);
            dos.writeUTF(entity.getId());
            dos.writeUTF(entity.getName());
            dos.flush();

            dos.close();
            ois.close();
            socket.close();
            serverSocket.close();
            System.out.println("===========服务端已关闭==========");
        }
    }
}
