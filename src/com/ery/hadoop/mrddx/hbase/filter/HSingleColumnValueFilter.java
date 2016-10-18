package com.ery.hadoop.mrddx.hbase.filter;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HSingleColumnValue 过滤对象
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * @author wanghao
 * @createDate 2013-1-29
 * @version v1.0
 */
public class HSingleColumnValueFilter implements IHFilter {
	public static final String KEYNAME_FAMILY = "family";
	public static final String KEYNAME_QUALIFIER = "qualifier";
	public static final String KEYNAME_COMPAREOP = "compareOp";
	public static final String KEYNAME_VALUE = "value";
	private byte[] family = null;
	private byte[] qualifier = null;
	private CompareOp compareOp = null;
	private byte[] value = null;
	public Filter filter;

	/**
	 * 构造方法
	 * 
	 * @param map
	 *            过滤参数
	 */
	public HSingleColumnValueFilter(Map<String, String> map) {
		if (map == null) {
			this.filter = null;
			// this.filter = new SingleColumnValueFilter();
			return;
		}

		Iterator<String> iterator = map.keySet().iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (KEYNAME_FAMILY.equalsIgnoreCase(key)) {
				family = Bytes.toBytes(map.get(key));
			} else if (KEYNAME_QUALIFIER.equalsIgnoreCase(key)) {
				qualifier = Bytes.toBytes(map.get(key));
			} else if (KEYNAME_COMPAREOP.equalsIgnoreCase(key)) {
				compareOp = getCompareOp(map.get(key));
			} else if (KEYNAME_VALUE.equalsIgnoreCase(key)) {
				value = Bytes.toBytes(map.get(key));
			}
		}

		this.filter = new SingleColumnValueFilter(family, qualifier, compareOp, value);
	}

	@Override
	public Filter getFilter() {
		return this.filter;
	}

	/**
	 * 获取CompareOp
	 * 
	 * @param name
	 *            操作名称
	 * @return CompareOp
	 */
	private CompareOp getCompareOp(String name) {
		if (CompareOp.LESS.toString().equals(name)) {
			return CompareOp.LESS;
		} else if (CompareOp.LESS_OR_EQUAL.toString().equals(name)) {
			return CompareOp.LESS_OR_EQUAL;
		} else if (CompareOp.EQUAL.toString().equals(name)) {
			return CompareOp.EQUAL;
		} else if (CompareOp.NOT_EQUAL.toString().equals(name)) {
			return CompareOp.NOT_EQUAL;
		} else if (CompareOp.GREATER_OR_EQUAL.toString().equals(name)) {
			return CompareOp.GREATER_OR_EQUAL;
		} else if (CompareOp.GREATER.toString().equals(name)) {
			return CompareOp.GREATER;
		} else if (CompareOp.NO_OP.toString().equals(name)) {
			return CompareOp.NO_OP;
		}

		return null;
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("SingleColumnValueFilter==>");
		strBuilder.append("family:");
		strBuilder.append(new String(family));
		strBuilder.append(", qualifier:");
		strBuilder.append(new String(qualifier));
		strBuilder.append(", compareOp:");
		strBuilder.append(compareOp.toString());
		strBuilder.append(", value:");
		strBuilder.append(new String(value));
		return strBuilder.toString();
	}
}