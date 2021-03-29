package edu.neu.bigdata.realtime.util;

import org.apache.commons.csv.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
*@Description CSV的工具类
**/
public class CSVUtil implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(CSVUtil.class);

    public static final String REGION_FILE = "areacode/dim_region.csv";

    public static final String PUB_FILE = "areacode/dim_pub.csv";

    public static final String PRODUCT_FILE = "areacode/dim_product.csv";

    public static final char QUOTE_COMMON = ',';

    /**
     * 读csv文件
     * @return
     * @throws Exception
     */
    public static List<Map<String,String>> readCSVFile(String path,char delimiter) throws Exception {
        List<Map<String,String>> results = new ArrayList<Map<String,String>>();
        Reader reader = null;
        try {
            reader = new InputStreamReader(CSVUtil.class.getClassLoader().getResourceAsStream(path));

            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withDelimiter(delimiter)
                    .withIgnoreHeaderCase()
                    .withTrim());

            Map<String,Integer> header = csvParser.getHeaderMap();
            Set<String> colKeys = header.keySet();
            //System.out.println("header=["+header+"]");
            //System.out.println("keys=["+colKeys+"]");

            for (CSVRecord csvRecord : csvParser) {
                Map<String,String> values = csvRecord.toMap();
                for(String colKey : colKeys){
                    String colValue = csvRecord.get(colKey);
                    values.put(colKey,colValue);
                }
                //System.out.println("values=["+values+"]");
                results.add(values);
            }

            //System.out.println("results=");

        }catch(Exception e) {
            log.error("read.csvfile.err=",path);
        }finally {
            if(null != reader){
                reader.close();
            }
        }

        return results;
    }

}
