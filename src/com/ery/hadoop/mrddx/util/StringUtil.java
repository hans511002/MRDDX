package com.ery.hadoop.mrddx.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sun.misc.BASE64Decoder;

import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.remote.FileRecordTrans.CompressType;
import com.ery.hadoop.mrddx.remote.OutputSourceRecordPO;

/**
 * String类型的工具类
 * 



 * @createDate 2013-1-14
 * @version v1.0
 */
public class StringUtil {
	public static final String DATE_FORMAT_TYPE1 = "yyyy-MM-dd HH:mm:ss";
	public static final String DATE_FORMAT_TYPE2 = "yyyyMMddHHmmss";
	public static final String DATE_FORMAT_TYPE3 = "yyyy-MM-dd";
	public static final String DOUBLE_FORMAT_pattern1 = "0";
	public static final String DOUBLE_FORMAT_pattern2 = "0.0";
	public static final String DOUBLE_FORMAT_pattern3 = "0.00";
	public static final String DOUBLE_FORMAT_6Decimal = "0.000000";
	public static final String DOUBLE_FORMAT_8Decimal = "0.00000000";
	public static final String DOUBLE_FORMAT_pattern4 = "#,##0";
	public static final String DOUBLE_FORMAT_pattern5 = "#,##0.0";
	public static final String DOUBLE_FORMAT_pattern6 = "#,##0.00";
	public static final String DOUBLE_FORMAT_pattern7 = "^[0-9]+$";

	/**
	 * String类型和数字类型比较
	 * 
	 * @param obj1
	 *            对象1
	 * @param obj2
	 *            对象2
	 * @return 返回最大值
	 */
	public static final Object getBigObject(Object obj1, Object obj2) {
		if (null == obj1 && null == obj2) {
			return null;
		}

		if (null != obj1 && null == obj2) {
			return obj1;
		}

		if (null == obj1 && null != obj2) {
			return obj2;
		}

		try {
			if (Double.parseDouble((String) obj1.toString()) > Double.parseDouble((String) obj2.toString())) {
				return obj1;
			} else {
				return obj2;
			}
		} catch (Exception e) {
		}

		// string比较
		if (obj1.toString().compareTo(obj2.toString()) > 0) {
			return obj1;
		} else {
			return obj2;
		}
	}

	/**
	 * String类型和数字类型比较：返回最小值
	 * 
	 * @param obj1
	 *            对象1
	 * @param obj2
	 *            对象2
	 * @return 返回最大值
	 */
	public static final Object getSmallObject(Object obj1, Object obj2) {
		if (null == obj1 && null == obj2) {
			return null;
		}

		if (null != obj1 && null == obj2) {
			return obj1;
		}

		if (null == obj1 && null != obj2) {
			return obj2;
		}

		try {
			if (Double.parseDouble((String) obj1.toString()) > Double.parseDouble((String) obj2.toString())) {
				return obj2;
			} else {
				return obj1;
			}
		} catch (Exception e) {
		}

		// string比较
		if (obj1.toString().compareTo(obj2.toString()) > 0) {
			return obj2;
		} else {
			return obj1;
		}
	}

	/**
	 * 将String转为int
	 * 
	 * @param obj
	 *            对象
	 * @return int
	 */
	public static final int objectToInt(Object obj) {
		if (null == obj) {
			return 0;
		}
		try {
			if (obj instanceof String) {
				return Integer.parseInt((String) obj);
			} else if (obj instanceof Integer) {
				return (Integer) obj;
			}
		} catch (Exception e) {
			return 0;
		}
		return 0;
	}

	/**
	 * 将String转为int
	 * 
	 * @param obj
	 *            对象
	 * @return int
	 */
	public static final String objectToString(Object obj, String defaultvalue) {
		if (null == obj) {
			return defaultvalue;
		}
		try {
			return obj.toString();
		} catch (Exception e) {
			return defaultvalue;
		}
	}

	/**
	 * 将String转为double
	 * 
	 * @param obj
	 *            对象
	 * @return double
	 */
	public static final double stringToDouble(Object obj) {
		try {
			if (obj instanceof String) {
				return Double.parseDouble((String) obj);
			}
		} catch (Exception e) {
			return 0.0;
		}
		return 0.0;
	}

	/**
	 * 将String转为double
	 * 
	 * @param obj
	 *            对象
	 * @return double
	 */
	public static final double stringToDouble(Object obj, int defaultValue) {
		try {
			if (obj instanceof String) {
				return Double.parseDouble((String) obj);
			}
		} catch (Exception e) {
			return defaultValue;
		}
		return defaultValue;
	}

	/**
	 * 将String转为int
	 * 
	 * @param obj
	 *            对象
	 * @param defaultValue
	 *            默认值
	 * @return int
	 */
	public static final int stringToInt(String obj, int defaultValue) {
		try {
			return Integer.parseInt(obj);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 获取时分秒的long型
	 * 
	 * @param mintime
	 *            时间(毫秒)
	 * @return 时分秒的long型
	 * @throws ParseException
	 *             异常
	 */
	static SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static long getTime(long mintime) throws ParseException {
		Date nowTime = new Date(mintime);
		String time = sdFormatter.format(nowTime);
		long tims = sdFormatter.parse(time).getTime();
		return tims / 1000;
	}

	/**
	 * 获取月份(yyyyMM)
	 * 
	 * @param date
	 *            日期
	 * @return 月份
	 * @throws ParseException
	 */
	public static String getMonthNo(Date date) throws ParseException {
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyyMM");
		return sdFormatter.format(date);
	}

	/**
	 * 获取天(yyyyMMdd)
	 * 
	 * @param date
	 *            日期
	 * @return 天
	 * @throws ParseException
	 *             异常
	 */
	public static String getDateNo(Date date) throws ParseException {
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyyMMdd");
		return sdFormatter.format(date);
	}

	/**
	 * 将double转为自定格式的字符串
	 * 
	 * @param d
	 *            double
	 * @return string
	 */
	public static final String doubleToString(Double d, String format) {
		DecimalFormat df = new DecimalFormat(format);
		return df.format(d);
	}

	/**
	 * date类型转换为String类型
	 * 
	 * @param data
	 *            Date类型的时间
	 * @param formatType
	 *            formatType格式为yyyy-MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒
	 * @return String
	 */
	public static String dateToString(Date data, String formatType) {
		if (null == data) {
			return "";
		}

		return new SimpleDateFormat(formatType).format(data);
	}

	/**
	 * ong类型转换为String类型
	 * 
	 * @param currentTime
	 *            要转换的long类型的时间
	 * @param formatType
	 *            要转换的string类型的时间格式
	 * @return String
	 * @throws ParseException
	 *             异常
	 */
	public static String longToString(long currentTime, String formatType) throws ParseException {
		Date dateOld = new Date(currentTime); // 根据long类型的毫秒数生命一个date类型的时间
		String sDateTime = dateToString(dateOld, formatType); // 把date类型的时间转换为string
		return sDateTime;
	}

	public static String stringToStrDate(String strTime) {
		Pattern pattern = Pattern.compile(DOUBLE_FORMAT_pattern7);
		// 检查是否为数字
		if (!matcher(strTime, pattern)) {
			return null;
		}

		// 检查是否满足长度
		if (strTime.length() > 14) {
			return null;
		}

		// 拼接长度满足14位
		if (strTime.length() < 14) {
			int len = 14 - strTime.length();
			for (int i = 0; i < len; i++) {
				strTime += 0;
			}
		}

		try {
			Date date = stringToDate(strTime, "yyyyMMddHHmmss");
			return dateToString(date, DATE_FORMAT_TYPE1);
		} catch (ParseException e) {
			return null;
		}
	}

	/**
	 * string类型转换为date类型
	 * 
	 * @param strTime要转换的string类型的时间
	 *            ，
	 * @param formatType要转换的格式yyyy
	 *            -MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒，
	 *            strTime的时间格式必须要与formatType的时间格式相同
	 * @return date
	 * @throws ParseException
	 */
	public static Date stringToDate(String strTime, String formatType) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat(formatType);
		Date date = null;
		date = formatter.parse(strTime);
		return date;
	}

	/**
	 * long转换为Date类型
	 * 
	 * @param currentTime要转换的long类型的时间
	 * @param formatType要转换的时间格式yyyy
	 *            -MM-dd HH:mm:ss//yyyy年MM月dd日 HH时mm分ss秒
	 * @return date
	 * @throws ParseException
	 *             异常
	 */
	public static Date longToDate(long currentTime, String formatType) throws ParseException {
		Date dateOld = new Date(currentTime); // 根据long类型的毫秒数生命一个date类型的时间
		String sDateTime = dateToString(dateOld, formatType); // 把date类型的时间转换为string
		Date date = stringToDate(sDateTime, formatType); // 把String类型转换为Date类型
		return date;
	}

	/**
	 * string类型转换为long类型
	 * 
	 * @param strTime要转换的String类型的时间
	 * @param formatType时间格式
	 *            ,strTime的时间格式和formatType的时间格式必须相同
	 * @return long
	 * @throws ParseException
	 *             异常
	 */
	public static long stringToLong(String strTime, String formatType) throws ParseException {
		Date date = stringToDate(strTime, formatType); // String类型转成date类型
		if (date == null) {
			return 0;
		} else {
			long currentTime = date.getTime(); // date类型转成long类型
			return currentTime;
		}
	}

	/**
	 * 转义正则表达则特殊字符
	 * 
	 * @param splitChar
	 *            字符
	 * @return 特殊字符
	 */
	public static String parseSpecialChar(String splitChar) {
		String res = splitChar;
		if (res.indexOf("|") >= 0) {
			res = res.replaceAll("\\|", "\\|");
		}
		if (res.indexOf("~") >= 0) {
			res = res.replaceAll("\\~", "\\~");
		}
		if (res.indexOf("^") >= 0) {
			res = res.replaceAll("\\^", "\\^");
		}
		if (res.indexOf("[") >= 0) {
			res = res.replaceAll("\\[", "\\[");
		}
		if (res.indexOf("]") >= 0) {
			res = res.replaceAll("\\]", "\\]");
		}
		if (res.indexOf("{") >= 0) {
			res = res.replaceAll("\\{", "\\{");
		}
		if (res.indexOf("}") >= 0) {
			res = res.replaceAll("\\}", "\\}");
		}
		if (res.indexOf("(") >= 0) {
			res = res.replaceAll("\\(", "\\(");
		}
		if (res.indexOf(")") >= 0) {
			res = res.replaceAll("\\)", "\\)");
		}
		return res;
	}

	/**
	 * 将long数组转为字符串数组
	 * 
	 * @param values
	 *            long数组
	 * @return 字符串数组
	 */
	public static String[] valueOfLongToString(long[] values) {
		String reValue[] = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			reValue[i] = String.valueOf(values[i]);
		}

		return reValue;
	}

	/**
	 * 将字符串数组转为long数组
	 * 
	 * @param values
	 *            字符串数组
	 * @return long数组
	 */
	public static long[] valueOfStringToLong(String[] values) {
		if (null == values) {
			return new long[0];
		}

		long reValue[] = new long[values.length];
		for (int i = 0; i < values.length; i++) {
			try {
				reValue[i] = Long.parseLong(values[i]);
			} catch (Exception e) {
				reValue[i] = -1;
			}
		}

		return reValue;
	}

	/**
	 * 将String数组转为为hashmap 例如： a1:12,c1:32,f3:dd 转为 [key=a1,value=12
	 * key=c1,value=32 key=f3,value=dd]
	 * 
	 * @param values
	 *            字符串
	 * @param perSig
	 *            分隔符
	 * @param subSig
	 *            子分隔符
	 * @return 分割后的列表
	 */
	public static Map<String, String> valueOfStringToHashMap(String value, String perSig, String subSig) {
		if (null == value || null == perSig || null == subSig) {
			return null;
		}

		Map<String, String> hashMap = new HashMap<String, String>();
		String keyValue[] = value.split(perSig);
		for (int i = 0; i < keyValue.length; i++) {
			String kv[] = keyValue[i].split(subSig);
			if (kv.length == 2 && null != kv[0] && null != kv[1]) {
				hashMap.put(kv[0], kv[1]);
			}
		}

		return hashMap;
	}

	/**
	 * 拼接字符串数组
	 * 
	 * @param content
	 *            字符串数组
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String toString(String[] content, String sign) {
		if (null == content) {
			return null;
		}

		sign = null == sign ? "," : sign;
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < content.length; i++) {
			strBuilder.append(content[i]);
			if (i < content.length - 1) {
				strBuilder.append(sign);
			}
		}

		return strBuilder.toString();
	}

	/**
	 * 获取异常日志内容
	 * 
	 * @param e
	 *            异常
	 * @return 异常内容
	 */
	public static String stringifyException(Throwable e) {
		return printStackTrace(e);
	}

	public static String printStackTrace(Throwable e) {
		StringWriter stm = new StringWriter();
		PrintWriter wrt = new PrintWriter(stm);
		e.printStackTrace(wrt);
		wrt.close();
		return stm.toString();
	}

	/**
	 * 截取查询sql语句 例如: select * from t_user where a=1; 返回'select * from'
	 * 
	 * @param sql
	 *            sql语句
	 * @return 截取的语句
	 */
	public static String subSqlSelectToFrom(String sql) {
		if (null == sql || sql.trim().length() <= 0) {
			return null;
		}

		String tmp = sql.trim().toUpperCase();
		int selectIndex = tmp.indexOf("SELECT");
		int fromIndex = tmp.indexOf("FROM");
		if (selectIndex <= -1 || fromIndex <= 7) {
			return sql = null;
		} else {
			return sql = sql.substring(selectIndex, fromIndex + 4);
		}
	}

	/**
	 * 将Object[]转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static LinkedList<Object> parseArrayToList(Object[] lstContent) {
		LinkedList<Object> lst = new LinkedList<Object>();
		for (int i = 0; i < lstContent.length; i++) {
			Object obj = lstContent[i];
			lst.add(obj);
		}

		return lst;
	}

	/**
	 * 将Object[]转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String parseArrayToString(Object[] lstContent, String sign) {
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < lstContent.length; i++) {
			Object obj = lstContent[i];
			if (null == obj) {
				strBuilder.append("null");
			} else {
				strBuilder.append(obj.toString());
			}

			if (i != lstContent.length - 1) {
				strBuilder.append(sign);
			}
		}

		return strBuilder.toString();
	}

	/**
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 将list转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String parseListToString(List<?> lstContent, String sign) {
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < lstContent.size(); i++) {
			Object obj = lstContent.get(i);
			if (null == obj) {
				strBuilder.append("null");
			} else {
				strBuilder.append(obj.toString());
			}

			if (i != lstContent.size() - 1) {
				strBuilder.append(sign);
			}
		}

		return strBuilder.toString();
	}

	/**
	 * 将list转换为String
	 * 
	 * @param lstContent
	 *            字符串列表
	 * @param sign
	 *            分隔符
	 * @return 拼接后的字符串
	 */
	public static String parseSetToString(Set<?> lstContent, String sign) {
		StringBuilder strBuilder = new StringBuilder();
		for (Object obj : lstContent) {
			if (null == obj) {
				strBuilder.append("null");
			} else {
				strBuilder.append(obj.toString());
			}
			strBuilder.append(sign);
		}

		return strBuilder.toString();
	}

	/**
	 * 将String数组转为Set集合
	 * 
	 * @param value
	 *            String数组
	 * @return set集合
	 */
	public static Set<String> parseStringArrayToSet(String[] value) {
		Set<String> set = new HashSet<String>();
		Collections.addAll(set, value);
		return set;
	}

	/**
	 * 将空格( )替换为下划线(_)
	 * 
	 * @param value
	 *            字符串
	 * @return 替换后的字符串
	 */
	public static String changeBlankToUnderline(String value) {
		if (null == value || value.trim().length() <= 0) {
			return "";
		}

		return value.replaceAll(" ", "_");
	}

	/**
	 * 获取唯一号码
	 * 
	 * @return uuid的前10位拼接上当前时间的毫秒数，再加密后的取前10位
	 * @throws NoSuchAlgorithmException
	 *             异常
	 */
	public static String getUniqueId() {
		UUID uuid = UUID.randomUUID();
		MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("MD5");
			md5.update((uuid.toString() + System.currentTimeMillis()).getBytes());
			return byte2hex(md5.digest()).substring(0, 10);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return uuid.toString().substring(0, 10);
	}

	/**
	 * 字节数组转为字符串
	 * 
	 * @param b
	 *            字节数组
	 * @return 字符串
	 */
	private static String byte2hex(byte[] b) // 二行制转字符串
	{
		String hs = "";
		String stmp = "";
		for (int n = 0; n < b.length; n++) {
			stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1) {
				hs = hs + "0" + stmp;
			} else {
				hs = hs + stmp;
			}
		}
		return hs.toUpperCase();
	}

	/**
	 * 将String数组转为大写
	 * 
	 * @param value
	 * @return
	 */
	public static String[] changeStringArrayToUpper(String[] value) {
		if (null == value) {
			return null;
		}
		String[] tmp = new String[value.length];
		for (int i = 0; i < value.length; i++) {
			if (null == value[i]) {
				tmp[i] = null;
				continue;
			}
			tmp[i] = value[i].toUpperCase();
		}

		return tmp;
	}

	public static boolean matcher(String value, Pattern pattern) {
		if (null == pattern) {
			return true;
		}

		return pattern.matcher(value).matches();
	}

	/**
	 * 后缀为斜杠的目录
	 * 
	 * @param path
	 * @return
	 */
	public static String getFileName(String path) {
		if (null == path || path.trim().length() <= 0) {
			return null;
		}
		int flag = path.lastIndexOf("/");
		if (flag != -1) {
			return path.substring(flag + 1, path.length());
		} else {
			return "";
		}
	}

	/**
	 * 返回parent路径，后缀没有"/"
	 * 
	 * @param path
	 * @return
	 */
	public static String getFilePathParentPath(String path) {
		if (null == path || path.trim().length() <= 0) {
			return null;
		}

		String paths[] = path.split("/");
		if (paths.length == 0) {
			return null;
		}

		if (path.startsWith("/")) {
			if (paths.length == 2) {
				return "/";
			} else if (paths.length > 2) {
				StringBuilder strBuid = new StringBuilder();
				strBuid.append("/");
				for (int i = 0; i < paths.length - 1; i++) {
					strBuid.append(paths[i]);
					if (i != 0 && i != paths.length - 2) {
						strBuid.append("/");
					}
				}

				return strBuid.toString();
			}
		} else {
			if (paths.length == 1) {
				return null;
			} else if (paths.length == 2) {
				return paths[0];
			} else if (paths.length > 2) {
				StringBuilder strBuid = new StringBuilder();
				for (int i = 0; i < paths.length - 1; i++) {
					strBuid.append(paths[i]);
					if (i != paths.length - 1) {
						strBuid.append("/");
					}
				}

				return strBuid.toString();
			}
		}

		return null;
	}

	public static void main(String[] args) {
		MRLog.systemOut(getFilePathParentPath("/asdas/dasdsa/sdadas/dsad/"));
	}

	/**
	 * 后缀为斜杠的目录
	 * 
	 * @param path
	 * @return
	 */
	public static String getSlashSuffixPath(String path) {
		if (null == path || path.trim().length() <= 0) {
			return null;
		}
		if (path.endsWith("/")) {
			return path;
		} else {
			return path + "/";
		}
	}

	public static String decodeString(String value, String beginSign, String endSign) {
		if (null == value || null == beginSign || null == endSign) {
			return null;
		}

		if (value.trim().length() < 2 && beginSign.trim().length() <= 0 && endSign.trim().length() <= 0) {
			return null;
		}

		if (!value.startsWith(beginSign) || !value.endsWith(endSign)) {
			return null;
		}

		return value.substring(1, value.length() - 1);
	}

	/**
	 * 
	 * @param path
	 * @param id
	 * @return
	 */
	public static String getFileOnlyKey(String path, int id) {
		return id + DataSHAUtil.md5Encrypt(id + path);
	}

	/**
	 * 
	 * @param outputHBaseOutColumnRelation
	 *            格式：[a:defvaluea],[b:defvalueb]
	 * @return
	 */

	public static Map<String, String> decodeOutColumnDefaultValue(String[] outputFieldDefaultValue) {
		Map<String, String> outColumnDefaultValue = new HashMap<String, String>();
		if (null == outputFieldDefaultValue || outputFieldDefaultValue.length <= 0) {
			return outColumnDefaultValue;
		}

		for (int i = 0; i < outputFieldDefaultValue.length; i++) {
			String temp = StringUtil.decodeString(outputFieldDefaultValue[i], "[", "]");
			if (null == temp) {
				continue;
			}

			String rela[] = temp.split(":");
			if (rela.length < 2) {
				continue;
			}

			outColumnDefaultValue.put(rela[0], temp.substring(temp.indexOf(":") + 1));
		}

		return outColumnDefaultValue;
	}

	/**
	 * 解析输出字段与列簇-列的拆分对应关系
	 * 
	 * @param hbaseColumnRelation
	 *            格式：[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]]
	 * @return
	 */
	public static void decodeOutColumnSplitRelation(String hbaseColumnRelation, List<String[]> list, List<String[]> rela) {
		if (null == hbaseColumnRelation) {
			return;
		}

		String[] str = hbaseColumnRelation.split("-");
		for (int i = 0; i < str.length; i++) {
			String temp = StringUtil.decodeString(str[i], "[", "]");
			if (null == temp) {
				continue;
			}

			String col[] = temp.split(":");
			if (col.length < 3) {
				continue;
			}

			String cluster = col[0];
			String column = col[1];
			String outputField = StringUtil.decodeString(col[2], "[", "]");
			if (null == outputField) {
				continue;
			}

			String field[] = outputField.split(",");
			if (field.length < 0) {
				continue;
			}

			rela.add(field);
			list.add(new String[] { cluster, column });
		}
	}

	public static String[] getNameValue(String tmpParam, String sign) {
		if (null == tmpParam || tmpParam.trim().length() <= 0) {
			return null;
		}

		if (null == sign) {
			return null;
		}

		tmpParam = tmpParam.trim();
		String tmp[] = tmpParam.split(sign);
		if (tmp.length != 2) {
			return null;
		}

		return tmp;
	}

	public static String serialObject(Object obj, boolean isGzip) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		String serStr = null;
		if (isGzip) {
			byte[] bts = GZIPUtils.zip(byteArrayOutputStream.toByteArray());
			serStr = new String(bts, "ISO-8859-1");
		} else {
			serStr = byteArrayOutputStream.toString("ISO-8859-1");
		}
		objectOutputStream.close();
		byteArrayOutputStream.close();
		return serStr;
	}

	private static final DecimalFormat decimalFormat;
	static {
		NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
		decimalFormat = (DecimalFormat) numberFormat;
		decimalFormat.applyPattern("#.##");
	}

	/**
	 * Given a full hostname, return the word upto the first dot.
	 * 
	 * @param fullHostname
	 *            the full hostname
	 * @return the hostname to the first dot
	 */
	public static String simpleHostname(String fullHostname) {
		int offset = fullHostname.indexOf('.');
		if (offset != -1) {
			return fullHostname.substring(0, offset);
		}
		return fullHostname;
	}

	private static DecimalFormat oneDecimal = new DecimalFormat("0.0");

	public static Object deserializeObject(String serStr, boolean isGzip, boolean urlEnCode) throws IOException {
		if (urlEnCode) {
			BASE64Decoder base64Decoder = new BASE64Decoder();
			serStr = new String(base64Decoder.decodeBuffer(serStr));
			// serStr = java.net.URLDecoder.decode(serStr, "UTF-8");
		}
		byte[] bts = serStr.getBytes("ISO-8859-1");
		if (isGzip)
			bts = GZIPUtils.unzip(bts);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bts);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			return objectInputStream.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	/**
	 * 通过后缀判断文件的压缩类型
	 * 
	 * @param path
	 * @return
	 */
	public static CompressType getCompressType(String path) {
		if (path == null || path.trim().length() <= 0) {
			return CompressType.NONE;
		}

		if (path.toLowerCase().endsWith(".tar.gz")) {
			return CompressType.TARGZ;
		} else if (path.toLowerCase().endsWith(".tar")) {
			return CompressType.TAR;
		} else if (path.toLowerCase().endsWith(".gz")) {
			return CompressType.GZ;
		}

		return CompressType.NONE;
	}

	/**
	 * 通过后缀判断文件的压缩类型
	 * 
	 * @param path
	 * @return
	 */
	public static int getCompressFlag(String path) {
		if (path == null || path.trim().length() <= 0) {
			return OutputSourceRecordPO.COMPRESS_NONE;
		}

		if (path.toLowerCase().endsWith(".gz")) {
			return OutputSourceRecordPO.COMPRESS_GZ;
		}

		return OutputSourceRecordPO.COMPRESS_NONE;
	}

	/**
	 * 获取java类的类名
	 */
	public static String getClassName(String value) {
		if (null == value || value.trim().length() <= 0) {
			return null;
		}

		String regex = "(?m)^\\s*public\\s+class\\s+(\\w+)\\b";
		Matcher m = Pattern.compile(regex).matcher(value);
		if (m.find()) {
			return m.group(1).trim();
		}
		return "";
	}

	public static String convertBLOBtoString(oracle.sql.BLOB BlobContent) {
		byte[] msgContent = BlobContent.getBytes(); // BLOB转换为字节数组
		byte[] bytes; // BLOB临时存储字节数组
		String newStr = ""; // 返回字符串
		int i = 1; // 循环变量
		long BlobLength; // BLOB字段长度
		try {
			BlobLength = BlobContent.length(); // 获取BLOB长度
			if (msgContent == null || BlobLength == 0) {// 如果为空，返回空值
				return "";
			} else {// 处理BLOB为字符串
				while (i < BlobLength) // 循环处理字符串转换，每次1024；Oracle字符串限制最大4k
				{
					bytes = BlobContent.getBytes(i, 1024);
					i = i + 1024;
					newStr = newStr + new String(bytes, "utf-8");
				}
				return newStr;
			}
		} catch (Exception e) {// oracle异常捕获
			e.printStackTrace();
		}
		return newStr;
	}

	private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
	private static final Random RANDOM = new Random();
	private static final char[] CHARS = { '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'q', 'w', 'e', 'r', 't',
			'y', 'u', 'i', 'o', 'p', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm',
			'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X',
			'C', 'V', 'B', 'N', 'M' };

	/**
	 * 字符串hash算法：s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1] <br>
	 * 其中s[]为字符串的字符数组，换算成程序的表达式为：<br>
	 * h = 31*h + s.charAt(i); => h = (h << 5) - h + s.charAt(i); <br>
	 * 
	 * @param start
	 *            hash for s.substring(start, end)
	 * @param end
	 *            hash for s.substring(start, end)
	 */
	public static long hash(String s, int start, int end) {
		if (start < 0) {
			start = 0;
		}
		if (end > s.length()) {
			end = s.length();
		}
		long h = 0;
		for (int i = start; i < end; ++i) {
			h = (h << 5) - h + s.charAt(i);
			// h = 31 * h + s.charAt(i);
		}
		return h;
	}

	public static byte[] encode(String src, String charset) {
		if (src == null) {
			return null;
		}
		try {
			return src.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			return src.getBytes();
		}
	}

	public static String decode(byte[] src, String charset) {
		return decode(src, 0, src.length, charset);
	}

	public static String decode(byte[] src, int offset, int length, String charset) {
		try {
			return new String(src, offset, length, charset);
		} catch (UnsupportedEncodingException e) {
			return new String(src, offset, length);
		}
	}

	public static String getRandomString(int size) {
		StringBuilder s = new StringBuilder(size);
		int len = CHARS.length;
		for (int i = 0; i < size; i++) {
			int x = RANDOM.nextInt();
			s.append(CHARS[(x < 0 ? -x : x) % len]);
		}
		return s.toString();
	}

	public static String safeToString(Object object) {
		try {
			return object.toString();
		} catch (Throwable t) {
			return "<toString() failure: " + t + ">";
		}
	}

	public static boolean isEmpty(String str) {
		return ((str == null) || (str.length() == 0));
	}

	public static byte[] hexString2Bytes(char[] hexString, int offset, int length) {
		if (hexString == null)
			return null;
		if (length == 0)
			return EMPTY_BYTE_ARRAY;
		boolean odd = length << 31 == Integer.MIN_VALUE;
		byte[] bs = new byte[odd ? (length + 1) >> 1 : length >> 1];
		for (int i = offset, limit = offset + length; i < limit; ++i) {
			char high, low;
			if (i == offset && odd) {
				high = '0';
				low = hexString[i];
			} else {
				high = hexString[i];
				low = hexString[++i];
			}
			int b;
			switch (high) {
			case '0':
				b = 0;
				break;
			case '1':
				b = 0x10;
				break;
			case '2':
				b = 0x20;
				break;
			case '3':
				b = 0x30;
				break;
			case '4':
				b = 0x40;
				break;
			case '5':
				b = 0x50;
				break;
			case '6':
				b = 0x60;
				break;
			case '7':
				b = 0x70;
				break;
			case '8':
				b = 0x80;
				break;
			case '9':
				b = 0x90;
				break;
			case 'a':
			case 'A':
				b = 0xa0;
				break;
			case 'b':
			case 'B':
				b = 0xb0;
				break;
			case 'c':
			case 'C':
				b = 0xc0;
				break;
			case 'd':
			case 'D':
				b = 0xd0;
				break;
			case 'e':
			case 'E':
				b = 0xe0;
				break;
			case 'f':
			case 'F':
				b = 0xf0;
				break;
			default:
				throw new IllegalArgumentException("illegal hex-string: " + new String(hexString, offset, length));
			}
			switch (low) {
			case '0':
				break;
			case '1':
				b += 1;
				break;
			case '2':
				b += 2;
				break;
			case '3':
				b += 3;
				break;
			case '4':
				b += 4;
				break;
			case '5':
				b += 5;
				break;
			case '6':
				b += 6;
				break;
			case '7':
				b += 7;
				break;
			case '8':
				b += 8;
				break;
			case '9':
				b += 9;
				break;
			case 'a':
			case 'A':
				b += 10;
				break;
			case 'b':
			case 'B':
				b += 11;
				break;
			case 'c':
			case 'C':
				b += 12;
				break;
			case 'd':
			case 'D':
				b += 13;
				break;
			case 'e':
			case 'E':
				b += 14;
				break;
			case 'f':
			case 'F':
				b += 15;
				break;
			default:
				throw new IllegalArgumentException("illegal hex-string: " + new String(hexString, offset, length));
			}
			bs[(i - offset) >> 1] = (byte) b;
		}
		return bs;
	}

	public static String dumpAsHex(byte[] src, int length) {
		StringBuilder out = new StringBuilder(length * 4);
		int p = 0;
		int rows = length / 8;
		for (int i = 0; (i < rows) && (p < length); i++) {
			int ptemp = p;
			for (int j = 0; j < 8; j++) {
				String hexVal = Integer.toHexString(src[ptemp] & 0xff);
				if (hexVal.length() == 1)
					out.append('0');
				out.append(hexVal).append(' ');
				ptemp++;
			}
			out.append("    ");
			for (int j = 0; j < 8; j++) {
				int b = 0xff & src[p];
				if (b > 32 && b < 127) {
					out.append((char) b).append(' ');
				} else {
					out.append(". ");
				}
				p++;
			}
			out.append('\n');
		}
		int n = 0;
		for (int i = p; i < length; i++) {
			String hexVal = Integer.toHexString(src[i] & 0xff);
			if (hexVal.length() == 1)
				out.append('0');
			out.append(hexVal).append(' ');
			n++;
		}
		for (int i = n; i < 8; i++) {
			out.append("   ");
		}
		out.append("    ");
		for (int i = p; i < length; i++) {
			int b = 0xff & src[i];
			if (b > 32 && b < 127) {
				out.append((char) b).append(' ');
			} else {
				out.append(". ");
			}
		}
		out.append('\n');
		return out.toString();
	}

	public static byte[] escapeEasternUnicodeByteStream(byte[] src, String srcString, int offset, int length) {
		if ((src == null) || (src.length == 0))
			return src;
		int bytesLen = src.length;
		int bufIndex = 0;
		int strIndex = 0;
		ByteArrayOutputStream out = new ByteArrayOutputStream(bytesLen);
		while (true) {
			if (srcString.charAt(strIndex) == '\\') {// write it out as-is
				out.write(src[bufIndex++]);
			} else {// Grab the first byte
				int loByte = src[bufIndex];
				if (loByte < 0)
					loByte += 256; // adjust for signedness/wrap-around
				out.write(loByte);// We always write the first byte
				if (loByte >= 0x80) {
					if (bufIndex < (bytesLen - 1)) {
						int hiByte = src[bufIndex + 1];
						if (hiByte < 0)
							hiByte += 256; // adjust for signedness/wrap-around
						out.write(hiByte);// write the high byte here, and
											// increment the index for the high
											// byte
						bufIndex++;
						if (hiByte == 0x5C)
							out.write(hiByte);// escape 0x5c if necessary
					}
				} else if (loByte == 0x5c) {
					if (bufIndex < (bytesLen - 1)) {
						int hiByte = src[bufIndex + 1];
						if (hiByte < 0)
							hiByte += 256; // adjust for signedness/wrap-around
						if (hiByte == 0x62) {// we need to escape the 0x5c
							out.write(0x5c);
							out.write(0x62);
							bufIndex++;
						}
					}
				}
				bufIndex++;
			}
			if (bufIndex >= bytesLen)
				break;// we're done
			strIndex++;
		}
		return out.toByteArray();
	}

	public static String toString(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return "";
		}
		StringBuffer buffer = new StringBuffer();
		for (byte byt : bytes) {
			buffer.append((char) byt);
		}
		return buffer.toString();
	}

	public static boolean equalsIgnoreCase(String str1, String str2) {
		if (str1 == null) {
			return str2 == null;
		}
		return str1.equalsIgnoreCase(str2);
	}

	public static int countChar(String str, char c) {
		if (str == null || str.isEmpty())
			return 0;
		final int len = str.length();
		int cnt = 0;
		for (int i = 0; i < len; ++i) {
			if (c == str.charAt(i)) {
				++cnt;
			}
		}
		return cnt;
	}

	public static String replaceOnce(String text, String repl, String with) {
		return replace(text, repl, with, 1);
	}

	public static String replace(String text, String repl, String with) {
		return replace(text, repl, with, -1);
	}

	public static String replace(String text, String repl, String with, int max) {
		if ((text == null) || (repl == null) || (with == null) || (repl.length() == 0) || (max == 0)) {
			return text;
		}
		StringBuffer buf = new StringBuffer(text.length());
		int start = 0;
		int end = 0;
		while ((end = text.indexOf(repl, start)) != -1) {
			buf.append(text.substring(start, end)).append(with);
			start = end + repl.length();
			if (--max == 0) {
				break;
			}
		}
		buf.append(text.substring(start));
		return buf.toString();
	}

	public static String replaceChars(String str, char searchChar, char replaceChar) {
		if (str == null) {
			return null;
		}
		return str.replace(searchChar, replaceChar);
	}

	public static String replaceChars(String str, String searchChars, String replaceChars) {
		if ((str == null) || (str.length() == 0) || (searchChars == null) || (searchChars.length() == 0)) {
			return str;
		}
		char[] chars = str.toCharArray();
		int len = chars.length;
		boolean modified = false;
		for (int i = 0, isize = searchChars.length(); i < isize; i++) {
			char searchChar = searchChars.charAt(i);
			if ((replaceChars == null) || (i >= replaceChars.length())) {// 删除
				int pos = 0;
				for (int j = 0; j < len; j++) {
					if (chars[j] != searchChar) {
						chars[pos++] = chars[j];
					} else {
						modified = true;
					}
				}
				len = pos;
			} else {// 替换
				for (int j = 0; j < len; j++) {
					if (chars[j] == searchChar) {
						chars[j] = replaceChars.charAt(i);
						modified = true;
					}
				}
			}
		}
		if (!modified) {
			return str;
		}
		return new String(chars, 0, len);
	}

}
