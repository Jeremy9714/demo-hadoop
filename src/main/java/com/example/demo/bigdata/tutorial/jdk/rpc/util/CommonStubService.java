package com.example.demo.bigdata.tutorial.jdk.rpc.util;

import com.example.demo.bigdata.common.entity.BaseService;
import com.example.demo.bigdata.tutorial.jdk.rpc.entity.RpcEntity;

import java.io.DataInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.Socket;

/**
 * @Description: 存根动态代理
 * @Author: Chenyang on 2024/10/18 17:00
 * @Version: 1.0
 */
public class CommonStubService<T> extends BaseService<T> {

    public CommonStubService(Class<T> clazz) {
        this.clazz = clazz;
    }

    /**
     * 动态代理客户端
     *
     * @return
     */
    public T getStub() {
        InvocationHandler handler = (proxy, method, args) -> {
            Socket socket = new Socket(InetAddress.getLocalHost(), 12345);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

            String methodName = method.getName();
            Class<?>[] parameterTypes = method.getParameterTypes();

            oos.writeUTF(methodName);
            oos.writeObject(parameterTypes);
            oos.writeObject(args);
            oos.flush();

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String id = dis.readUTF();
            String name = dis.readUTF();
//            T entity = clazz.getConstructor(String.class, String.class).newInstance(id, name);
            RpcEntity entity = new RpcEntity();
            entity.setId(id);
            entity.setName(name);

            dis.close();
            oos.close();
            socket.close();
            return entity;
        };

        Object obj = Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, handler);
        return (T) obj;
    }
}
