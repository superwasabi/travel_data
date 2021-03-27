package com.qianfeng.bigdata.realtime.dvo;

import java.io.Serializable;
/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description 经纬度信息封装模型
**/
public class GisDO implements Serializable {

    private String longitude;
    private String latitude;
    private String adcode;
    private String province;
    private String district;
    private String address;

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getAdcode() {
        return adcode;
    }

    public void setAdcode(String adcode) {
        this.adcode = adcode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
