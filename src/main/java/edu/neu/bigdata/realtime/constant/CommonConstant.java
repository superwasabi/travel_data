package edu.neu.bigdata.realtime.constant;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
*@Description 项目使用的常量类
**/
public class CommonConstant implements Serializable {

    public static final int DEF_NUMBER_ZERO = 0;
    public static final int DEF_NUMBER_ONE = 1;
    public static final int DEF_NUMBER_DUL = 2;

    //用户数量限制级别
    public static final Integer USER_COUNT_LEVEL = 5;

    //时间格式
    public static final String FORMATTER_YYYYMMDD = "yyyyMMdd";
    public static final String FORMATTER_YYYYMMDDHHMMDD = "yyyyMMddHHmmss";

    public static final String KAFKA_PRODUCER_JSON_PATH = "kafka/json/kafka-producer.properties";

    public static final String ZK_CONNECT = "zk.connect";
    public static final String ZK_CONNECT_KAFKA = "zk.kafka.connect";
    public static final String ZK_SESSION_TIMEOUT = "zk.session.timeout";
    public static final String ZK_CONN_TIMEOUT = "zk.connection.timeout";
    public static final String ZK_BEE_ROOT = "zk.dw.root";
}
