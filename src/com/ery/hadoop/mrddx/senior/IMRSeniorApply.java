package com.ery.hadoop.mrddx.senior;

import com.ery.hadoop.mrddx.DBRecord;

/**
 * MR的高级应用接口
 * 



 * @createDate 2013-2-18
 * @version v1.0
 */
public interface IMRSeniorApply {
	// 常量（提供的运算: + - * / %）
	public static final String TYPE_OPERATION_DIGIT = "digit";
	
	// 数据过滤
	public static final String TYPE_OPERATION_FILTER = "filter";
	
	// 字段值的替换
	public static final String TYPE_OPERATION_REPLACE = "replace";
	
	// 使用文件名字
	public static final String TYPE_OPERATION_FILENAME = "filename";
	
	// 字段值反转
	public static final String TYPE_OPERATION_REVERSE = "reverse";
	
	// 目标字段值的替换
	public static final String TYPE_OPERATION_CONDITION_REPLACE = "conreplace";

	/**
	 * 应用
	 * 
	 * @param dbRecord
	 *            记录
	 */
	public void apply(DBRecord dbRecord, Object obj);
	
	/**
	 * 初始化规则
	 * @param rule
	 */
	public void initRule(String rule);
}
