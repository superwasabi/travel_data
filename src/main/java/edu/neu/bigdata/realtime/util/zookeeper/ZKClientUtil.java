package edu.neu.bigdata.realtime.util.zookeeper;

import edu.neu.bigdata.realtime.constant.CommonConstant;
import edu.neu.bigdata.realtime.util.PropertyUtil;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.security.JaasUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
*@Description zookeeper的工具
**/
public class ZKClientUtil implements Serializable {

    public static final String ZK_PATH_SEPTAL = "/";

    private static ZkClient zkClient;
    private static ZkUtils zkUtils;

    private static String zkKafkaUrl;
    private static String zkUrl;
    private static Integer zkSessionTimeout = 60000;
    private static Integer zkConnTimeout = 10000;
    public static String zkBeeRoot;

    public static final  String zkConf = "zk/zk.properties";

    static{
        Properties zkProperty = PropertyUtil.readProperties(ZKClientUtil.zkConf);

        zkKafkaUrl = zkProperty.getProperty(CommonConstant.ZK_CONNECT_KAFKA);
        zkUrl = zkProperty.getProperty(CommonConstant.ZK_CONNECT);
        zkBeeRoot = zkProperty.getProperty(CommonConstant.ZK_BEE_ROOT);
        zkSessionTimeout = Integer.valueOf(zkProperty.getProperty(CommonConstant.ZK_SESSION_TIMEOUT)) * 1000;
        zkConnTimeout = Integer.valueOf(zkProperty.getProperty(CommonConstant.ZK_CONN_TIMEOUT)) * 1000;

        Validate.notEmpty(zkUrl, "zkUrl must a not empty");
        Validate.notNull(zkSessionTimeout, "zkSessionTimeout must a not empty");
        Validate.notNull(zkConnTimeout, "zkConnTimeout must a not empty");
    }

    /**
     * 连接zookeeper
     */
    public static synchronized ZkClient connZKClient() throws Exception{
        if(null == zkClient){
            zkClient = new ZkClient(
                    zkUrl,
                    zkSessionTimeout,
                    zkConnTimeout,
                    ZKStringSerializer$.MODULE$);
        }
        return zkClient;
    }


    public static synchronized ZkClient connZKClient4Kafka() throws Exception{
        if(null == zkClient){
            zkClient = new ZkClient(
                    zkKafkaUrl,
                    zkSessionTimeout,
                    zkConnTimeout,
                    ZKStringSerializer$.MODULE$);
        }
        return zkClient;
    }

    /**
     * 判断zk节点是否存在
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean exist(String path) throws Exception{
        ZkClient zkClient = connZKClient();
        if(null != zkClient){
            return zkClient.exists(path);
        }
        return false;
    }
}
