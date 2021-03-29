package edu.neu.bigdata.realtime.enumes;

import java.util.Arrays;
import java.util.List;
/**
*@Description 操作系统枚举   -- 没有具体版本(如果再细还有系统对应的版本)
**/
public enum OSEnum {

    ANDROID("1", "安卓"),
    IOS("2", "苹果"),
    OTHER("9", "其他");


    private String code;
    private String desc;

    private OSEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getOS(){
        List<String> oss = Arrays.asList(
                ANDROID.code,
                IOS.code
        );
        return oss;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
