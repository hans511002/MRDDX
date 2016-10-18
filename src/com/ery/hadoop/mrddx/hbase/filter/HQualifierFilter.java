package com.ery.hadoop.mrddx.hbase.filter;

import java.util.Map;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * HQualifier 过滤对象
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-29
 * @version v1.0
 */
public class HQualifierFilter implements IHFilter {
	/**
	 * 构造方法
	 * 
	 * @param map
	 *            过滤参数
	 */
	public HQualifierFilter(Map<String, String> map) {

	}

	@Override
	public Filter getFilter() {
		return null;
	}
}
