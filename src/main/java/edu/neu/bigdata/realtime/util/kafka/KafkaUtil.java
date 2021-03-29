package edu.neu.bigdata.realtime.util.kafka;

import edu.neu.bigdata.realtime.util.zookeeper.ZKClientUtil;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
*@Description kafka工具类
**/
public class KafkaUtil {
    /**
     * 读取配置文件
     * @return
     */
    public static Properties readKafkaProps(String path) throws IOException{
        Properties props = new Properties();
        props.load(KafkaUtil.class.getClassLoader().getResourceAsStream(path));
        return props;
    }
    //-----------------------------------------------

    /**
     * 创建生产者
     * @return
     */
    public static KafkaProducer<String, String> createProducer(String path) throws IOException {
        Validate.notEmpty(path, "path must be not empty");
        KafkaProducer<String, String>  producer = null;
        Properties props = readKafkaProps(path);
        if(null != props){
            producer = new KafkaProducer<String, String>(props);
        }
        return producer;
    }
}
