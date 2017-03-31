package com.ery.hadoop.mrddx.remote;

import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**



 * @createDate 2013-1-7
 * @version v1.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface FTPWritable {
	/**
	 * 从数据库读取数据
	 * 
	 * @param resultSet
	 *            结果集
	 * @param fieldNames
	 *            字段名
	 * @throws SQLException
	 *             SQL异常
	 */
	public void readFields(SplitPathPO values, String[] fieldNames);

	/**
	 * 获取字段名
	 * 
	 * @return 字段名列表
	 */
	public String[] getFieldNames();
}
