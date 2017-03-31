package com.ery.hadoop.mrddx.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**


 * 

 * @version v1.0
 * @create Data 2013-1-9
 */
public class FTPRecord implements FTPWritable, Writable, WritableComparable<Object> {

	/**
	 * 行数据对应列的名称和值
	 */
	private Map<String, Object> row = new HashMap<String, Object>();

	/**
	 * 构造方法
	 */
	public FTPRecord() {

	}

	/**
	 * 构造方法
	 * 
	 * @param row
	 *            记录
	 */
	public FTPRecord(Map<String, Object> row) {
		this.row = row;
	}

	/**
	 * 构造方法
	 * 
	 * @param names
	 *            字段名称
	 * @param values
	 *            字段值
	 */
	public FTPRecord(String[] names, Object[] values) {
		this.row.clear();
		for (int i = 0; i < names.length; i++) {
			if (values.length > i) {
				this.row.put(names[i], values[i]);
			} else {
				this.row.put(names[i], null);
			}
		}
	}

	/**
	 * 根据字段名称值
	 * 
	 * @param key
	 *            字段名称
	 * @return 值
	 */
	public Object getData(String key) {
		return this.row.get(key);
	}

	/**
	 * 判断是否包含字段
	 * 
	 * @param key
	 *            字段名称
	 * @return true表示包含，否则不包含
	 */
	public boolean containsKeyData(String key) {
		return this.row.containsKey(key);
	}

	/**
	 * 移除字段
	 * 
	 * @param key
	 *            字段名称
	 * @return 被移除字段的值
	 */
	public Object removeData(String key) {
		return this.row.remove(key);
	}

	@Override
	public String[] getFieldNames() {
		return this.row.keySet().toArray(new String[0]);
	}

	/**
	 * 清空记录
	 */
	public void clear() {
		this.row.clear();
	}

	/**
	 * 获取当前记录
	 * 
	 * @return 记录
	 */
	public Map<String, Object> getRow() {
		return row;
	}

	@Override
	public boolean equals(Object o) {
		return toString().equals(o.toString());
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (String key : this.row.keySet()) {
			buf.append(key);
			buf.append(":");
			buf.append(this.row.get(key) + ",");
		}

		if (buf.length() > 0) {
			buf.deleteCharAt(buf.length() - 1);
		}
		return buf.toString();
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public int compareTo(Object o) {
		return this.toString().compareTo(o.toString());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringBuilder strBuilder = new StringBuilder();
		for (String name : this.row.keySet()) {
			strBuilder.append(name);
			strBuilder.append(":");
			Object obj = this.row.get(name);
			if (null != obj) {
				strBuilder.append(obj.toString());
			}
			strBuilder.append(",");
		}

		String tmp = strBuilder.toString();
		if (tmp.length() == 0) {
			out.writeInt(0);
			return;
		}

		tmp = tmp.substring(0, strBuilder.length() - 1);
		byte[] byteValue = tmp.getBytes();
		out.writeInt(byteValue.length);
		out.write(byteValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.row.clear();
		byte bt[] = null;
		try {
			int len = in.readInt();
			if (len <= 0) {
				return;
			}

			bt = new byte[len];
			in.readFully(bt, 0, len);
		} catch (Exception e) {
			return;
		}

		String[] vals = new String(bt).split(",");
		for (int i = 0; i < vals.length; i++) {
			String[] tp = vals[i].split(":", 2);
			if (tp.length == 2) {
				this.row.put(tp[0], tp[1]);
			}
		}
	}

	@Override
	public void readFields(SplitPathPO values, String[] fieldNames) {
		for (int i = 0; i < fieldNames.length; i++) {
			switch (i) {
			case 0:
				this.row.put(fieldNames[i], values.getRecord());
				break;
			default:
				break;
			}
		}
	}

	/**
	 * 当前记录是否为空
	 * 
	 * @return true表示为空
	 */
	public boolean isNull() {
		return this.row.size() == 0;
	}
}
