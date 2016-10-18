package com.ery.hadoop.mrddx.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * hbase rowkey 工具类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-16
 * @version v1.0
 */
public class HBaseRowKeyRule {
	public static final String DATE_RULE_YYYY_MM_DD = "yyyy-MM-dd";
	public static final String DATE_RULE_YYYY_MM_DD_HH = "yyyy-MM-dd HH";
	public static final String DATE_RULE_YYYY_MM_DD_HH_MM = "yyyy-MM-dd HH:mm";
	public static final String DATE_RULE_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
	public static SimpleDateFormat FORMATTER_YYYY_MM_DD = new SimpleDateFormat(DATE_RULE_YYYY_MM_DD);
	public static SimpleDateFormat FORMATTER_YYYY_MM_DD_HH = new SimpleDateFormat(DATE_RULE_YYYY_MM_DD_HH);
	public static SimpleDateFormat FORMATTER_YYYY_MM_DD_HH_MM = new SimpleDateFormat(DATE_RULE_YYYY_MM_DD_HH_MM);
	public static SimpleDateFormat FORMATTER_YYYY_MM_DD_HH_MM_SS = new SimpleDateFormat(DATE_RULE_YYYY_MM_DD_HH_MM_SS);

	/**
	 * 获取行key
	 * 
	 * @param value
	 *            值
	 * @param rule
	 *            规则
	 * @return 行key
	 */
	public static String getValueByRule(String value, String rule) {
		if (null == value || value.trim().length() <= 0) {
			return "";
		}

		value = value.trim();
		rule = rule.trim();

		// 1:转为date
		Date date = parseStringToDate(value, rule);
		if (null == date) {
			return "";
		}

		// 2:date转为String
		if (DATE_RULE_YYYY_MM_DD.equals(rule)) {
			return FORMATTER_YYYY_MM_DD.format(date);
		}
		if (DATE_RULE_YYYY_MM_DD_HH.equals(rule)) {
			return FORMATTER_YYYY_MM_DD_HH.format(date);
		}
		if (DATE_RULE_YYYY_MM_DD_HH_MM.equals(rule)) {
			return FORMATTER_YYYY_MM_DD_HH_MM.format(date);
		} else if (DATE_RULE_YYYY_MM_DD_HH_MM_SS.equals(rule)) {
			return FORMATTER_YYYY_MM_DD_HH_MM_SS.format(date);
		}

		return "";
	}

	/**
	 * String转为日期
	 * 
	 * @param value
	 *            时间字符串
	 * @param rule
	 *            规则
	 * @return 日期Date
	 */
	public static Date parseStringToDate(String value, String rule) {
		if (null == value || value.trim().length() <= 0) {
			return null;
		}

		value = value.trim();
		rule = rule.trim();

		try {
			if (DATE_RULE_YYYY_MM_DD.equals(rule)) {
				return FORMATTER_YYYY_MM_DD.parse(value);
			}
			if (DATE_RULE_YYYY_MM_DD_HH.equals(rule)) {
				return FORMATTER_YYYY_MM_DD_HH.parse(value);
			}
			if (DATE_RULE_YYYY_MM_DD_HH_MM.equals(rule)) {
				return FORMATTER_YYYY_MM_DD_HH_MM.parse(value);
			}
			if (DATE_RULE_YYYY_MM_DD_HH_MM_SS.equals(rule)) {
				return FORMATTER_YYYY_MM_DD_HH_MM_SS.parse(value);
			}
		} catch (ParseException e) {
			return null;
		}

		return null;
	}
}
