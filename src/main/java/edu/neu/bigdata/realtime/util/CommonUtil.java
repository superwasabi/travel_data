package edu.neu.bigdata.realtime.util;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
*@Description 通用工具类
**/
public class CommonUtil {

	private CommonUtil() {
	}
		/**
	 * 随机生成11位手机号码
	 * 手机号码分为三段,first：3位，second：4位，third：4位
	 * @return
	 */
	public static String getMobile() {
		String[] telFirst="134,135,136,137,138,139,150,151,152,153,155,156,157,158,159,130,131,132,133,171,172,173,175,176,177,178,181,182,183,185,186,187,188,189,199".split(",");
		int index=(int)(Math.random()*telFirst.length);
		String first=telFirst[index];
		String second=String.valueOf((int)(Math.random()*10000)+10000).substring(1);
		String thrid=String.valueOf((int)(Math.random()*10000)+10000).substring(1);
		return first+second+thrid;
	}


	/**
	 * 模拟产品信息
	 * @return
	 */
	public static List<String> getProducts() {
		List<String> products = new ArrayList<String>();
		int count = 100;
		for(int i=1; i<=count; i++){
			products.add("P"+i);
		}
		return products;
	}


	public static String getRadomIP() {
		List<Integer> ipNumbers = getRangeNumeric(1,255,1);
		StringBuffer buffer = new StringBuffer();
		for(int i=1;i<=4;i++){
			int num = getRandomElementRange(ipNumbers);
			buffer.append(num);
			if(i != 4){
				buffer.append(".");
			}
		}
		String ip = buffer.toString();
		return ip;
	}



	public static long getRandomTimestamp4Release(long ct,int type,int range){
		Date dt = new Date(ct);
		int hour = new Random().nextInt(23);
		int minute = new Random().nextInt(59);
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		int addVal = new Random().nextInt(range) + 1;
		if(Calendar.SECOND == type){
			cal.add(Calendar.SECOND, addVal);
		}else if(Calendar.MINUTE == type){
			cal.add(Calendar.MINUTE, addVal);
		}else if(Calendar.HOUR_OF_DAY == type){
			cal.add(Calendar.HOUR_OF_DAY, addVal);
		}else if(Calendar.DATE == type){
			cal.add(Calendar.DATE, addVal);
		}
		return cal.getTimeInMillis();
	}

	public static List<String> getRangeNumber(int begin, int end, int sep){
		List<String> rangeNums = new ArrayList<String>();
		for(int i=begin; i<=end; i+=sep){
			rangeNums.add(String.valueOf(i));
		}
		return rangeNums;
	}


	public static List<Integer> getRangeNumeric(int begin, int end, int sep){
		List<Integer> rangeNums = new ArrayList<Integer>();
		for(int i=begin; i<=end; i+=sep){
			rangeNums.add(i);
		}
		return rangeNums;
	}

	//===20190301===========================================================

	public static String getRandom(Integer count){
		String s = "0123456789abcdefghijkmnopqrstuvwxz";
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<count;i++){
			int idx = new Random().nextInt(22)+1;
			sb.append(s.charAt(idx));
		}
		return sb.toString();
	}
	public static long getSelectTimestamp(String cTimes, String formater){
		Date dt = parseText(cTimes,formater);
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		return cal.getTimeInMillis();
	}
	public static String getRandomChar(int count){
		String s = "abcdefghijkmnopqrstuvwxz";
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<count;i++){
			int idx = new Random().nextInt(22)+1;
			sb.append(s.charAt(idx));
		}
		return sb.toString();
	}

	public static String getRandomNumStr(int count){
		String range = "123456789";
		int len = range.length();
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<count;i++){
			int idx = new Random().nextInt(10)+1;
			if(idx >= len){
				sb.append(range.charAt(idx-len));
			}else {
				sb.append(range.charAt(idx));
			}
		}
		return sb.toString();
	}

	public static <T> T getRandomElementRange(List<T> elements){
		T element = null;
		if(null != elements){
			int size = elements.size();
			int idx = new Random().nextInt(size);
			if(idx >= size){
				idx = size -1;
			}
			element = (T)elements.get(idx);
		}
		return element;
	}

	public static List<String> getRandomSubElementRange(List<String> elements, int count){
		List<String> subElements = new ArrayList<String>();
		if(null != elements){
			int size = elements.size();
			if(size >= count){
				for(int i=1; i<=count; i++){
					int idx = new Random().nextInt(size-1)+1;
					if(idx >= size){
						idx = size -1;
					}
					subElements.add(elements.get(idx));
				}
			}
		}
		return subElements;
	}

	public static int getRandomNum(int count){
		int num = 0;
		try{
			String range = "123456789";
			int len = range.length();
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<count;i++){
				int idx = new Random().nextInt(10)+1;
				if(idx >= len){
					sb.append(range.charAt(idx-len));
				}else {
					sb.append(range.charAt(idx));
				}
			}
			String numstr = sb.toString();
			num = Integer.valueOf(numstr);
		}catch(Exception e){
			e.printStackTrace();
		}

		return num;
	}

	//---日期相关----------------------------------------------

	/**
	 * 日期格式化
	 */
	public static String formatDate4Timestamp(Long ct, String type) {
		SimpleDateFormat sdf = new SimpleDateFormat(type);
		String result = null;
		try {
			if (null != ct) {
				Calendar cal = Calendar.getInstance();
				cal.setTimeInMillis(ct);
				result = sdf.format(cal.getTime());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 日期格式化
	 * @param date
	 * @return
	 */
	public static String formatDate(Date date, String type) {
		SimpleDateFormat sdf = new SimpleDateFormat(type);
		String result = null;
		try {
			if (null != date) {
				result = sdf.format(date);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static String formatDate4Def(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String result = null;
		try {
			if (null != date) {
				result = sdf.format(date);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 文本转时间
	 * 
	 * @param content
	 * @return
	 */
	public static Date parseText(String content, String dateType) {
		Date date = null;
		if (!StringUtils.isEmpty(content)) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(dateType);
				date = sdf.parse(content);
			} catch (ParseException pe) {
				pe.printStackTrace();
			}
		}
		return date;
	}

	public static Date getFormatTime(String dt, int diff, int type,String formater){
		Date result = null;
		if(StringUtils.isNotEmpty(dt)){
			Date tmp = parseText(dt, formater);
			Calendar cal = Calendar.getInstance();
			cal.setTime(tmp);
			cal.add(type, diff);
			result = cal.getTime();
		}
		return result;
	}

	public static String computeFormatTime(String dt, int diff, int type,String formater){
		Date cTime = getFormatTime(dt, diff, type,formater);
		return formatDate(cTime, formater);
	}

	public static Long calculateTimeDiffDay(String dateFormat, String begin, String end,int calType) {
		Long diff = 0L;
		if(StringUtils.isNotEmpty(dateFormat) && StringUtils.isNotEmpty(begin) && StringUtils.isNotEmpty(end)){
			Date beginTime = parseText(begin, dateFormat);
			Date endTime = parseText(end, dateFormat);
			long tmp = beginTime.getTime() - endTime.getTime();

			if(Calendar.DATE == calType){
				diff = Math.abs(tmp /(24 * 60 * 60 * 1000));
			}else if(Calendar.HOUR_OF_DAY == calType){
				diff =  Math.abs(tmp /(60 * 60 * 1000));
			}else if(Calendar.MINUTE == calType){
				diff =  Math.abs(tmp /(60 * 1000));
			}else if(Calendar.SECOND == calType){
				diff =  Math.abs(tmp /(1000));
			}
		}
		//System.out.println("两个时间之间的差为：" + diff);
		return diff;
	}


	/**
	 * MD5处理
	 * @param key the key to hash (variable length byte array)
	 * @return MD5 hash as a 32 character hex string.
	 */
	public static String getMD5AsHex(byte[] key) {
		return getMD5AsHex(key, 0, key.length);
	}

	/**
	 * MD5处理
	 * @param key the key to hash (variable length byte array)
	 * @param offset
	 * @param length
	 * @return MD5 hash as a 32 character hex string.
	 */
	private static String getMD5AsHex(byte[] key, int offset, int length) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key, offset, length);
			byte[] digest = md.digest();
			return new String(Hex.encodeHex(digest));
		} catch (NoSuchAlgorithmException e) {
			// this should never happen unless the JDK is messed up.
			throw new RuntimeException("Error computing MD5 hash", e);
		}
	}
}
