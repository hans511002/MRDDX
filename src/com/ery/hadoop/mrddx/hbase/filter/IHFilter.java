package com.ery.hadoop.mrddx.hbase.filter;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * HBase过滤的统一接口
 * 



 * @createDate 2013-1-29
 * @version v1.0
 */
public interface IHFilter {
	/**
	 * 获取filter
	 * 
	 * @return 过滤对象
	 */
	public Filter getFilter();

	/**
	 * 获取日志信息
	 * 
	 * @return 日志信息
	 */
	public String toString();
}
