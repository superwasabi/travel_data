package edu.neu.bigdata.realtime.util;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
*@Description 反射工具类
**/
public class ReflexUtil implements Serializable {

    public static Map<String,Class>  datatypes = new HashMap<String,Class>();
    static {
        datatypes.put(Integer.class.getName(), Integer.class);
        datatypes.put(Long.class.getName(), Long.class);
        datatypes.put(String.class.getName(), String.class);
        datatypes.put(Boolean.class.getName(), Boolean.class);
        datatypes.put(List.class.getName(), List.class);
        datatypes.put(Map.class.getName(), Map.class);
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;


    public static void setFildKeyValues4SingleString(Object obj, String fieldName, Object fieldValue) throws Exception{
        if(null != obj){
            Class cls = obj.getClass();
            String methodName = "set"+ StringUtils.capitalize(fieldName);
            Method method = cls.getMethod(methodName,String.class);
            method.invoke(obj, fieldValue);
        }
    }

}

