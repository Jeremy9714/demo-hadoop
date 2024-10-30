package com.example.demo.bigdata.tutorial.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @Description: hive udf
 * @Author: Chenyang on 2024/10/30 10:03
 * @Version: 1.0
 */
public class MyLength extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 入参元数据信息
        if (arguments.length != 1) {
            throw new UDFArgumentException("参数个数必须为1");
        }

        ObjectInspector argument = arguments[0];
        if (argument.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("参数必须为基本类型");
        }

        // 基本数据类型
        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) argument;
        if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("参数必须为String类型");
        }

        // 返回值元数据信息
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject argument = arguments[0];
        // 获取入参(懒加载)
        Object obj = argument.get();

        if (obj == null) {
            return 0;
        } else {
            return obj.toString().length();
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
