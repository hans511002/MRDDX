package com.ery.hadoop.mrddx.hbase.filter;

import java.util.Map;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * HKeyOnly 过滤对象
 * 



 * @createDate 2013-1-29
 * @version v1.0
 */
public class HKeyOnlyFilter implements IHFilter {
	/**
	 * 构造方法
	 * 
	 * @param map
	 *            过滤参数
	 */
	public HKeyOnlyFilter(Map<String, String> map) {

	}

	@Override
	public Filter getFilter() {
		return null;
	}
}
