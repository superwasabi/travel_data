package edu.neu.bigdata.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.jayway.jsonpath.JsonPath;
import edu.neu.bigdata.realtime.constant.CommonConstant;
import edu.neu.bigdata.realtime.dvo.GisDO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.*;

/**
*@Description 高德工具类
**/
public class AmapGisUtil implements Serializable {
    public static final String GIS_CSV_FILE = "areacode/china_gis_location.csv";

    public static List<GisDO> GL_GISDO = new ArrayList<GisDO>();
    public static List<GisDO> initDatas(){
        try{
            if(CollectionUtils.isEmpty(GL_GISDO)){
                List<Map<String,String>> datas = CSVUtil.readCSVFile(GIS_CSV_FILE, CSVUtil.QUOTE_COMMON);
                for(Map<String,String> data : datas){
                    GisDO gisDO = new GisDO();

                    for(Map.Entry<String,String> entry : data.entrySet()){
                        String key = entry.getKey();
                        String methodKey = StringUtils.capitalize(key);
                        String value = entry.getValue();

                        ReflexUtil.setFildKeyValues4SingleString(gisDO, key, value);
                    }

                    GL_GISDO.add(gisDO);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return GL_GISDO;
    }
}
