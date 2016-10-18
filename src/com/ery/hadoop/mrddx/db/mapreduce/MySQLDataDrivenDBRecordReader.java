package com.ery.hadoop.mrddx.db.mapreduce;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBInputFormat;

/**
 * 
 * Copyrights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 针对MYSQL的数据类型驱动(Data-Driver)拆分RecordReader实现类
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 * 
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MySQLDataDrivenDBRecordReader<T extends DBWritable> extends DataDrivenDBRecordReader<T> {
	/**
	 * 构造方法
	 * 
	 * @param split
	 *            拆分对象
	 * @param inputClass
	 *            记录类型
	 * @param conf
	 *            配置对象
	 * @param conn
	 *            数据库连接对象
	 * @param dbConfig
	 *            配置对象
	 * @param cond
	 *            查询条件表达式
	 * @param fields
	 *            查询字段名称
	 * @param table
	 *            表名
	 */
	public MySQLDataDrivenDBRecordReader(InputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
			DBConfiguration dbConfig, String cond, String[] fields, String table) {
		super(split, inputClass, conf, conn, dbConfig, cond, fields, table, "MYSQL");
	}

	@Override
	protected ResultSet executeQuery(DBInputFormat.DBInputSplit split) throws SQLException {
		this.statement = getConnection().prepareStatement(getSelectQuery(), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		// 注:缓冲设置过大，容易出现内存溢出.
		int fetchSize = this.dbConf.getInputQueryFetchSize();
		this.statement.setFetchSize(fetchSize > 0 ? fetchSize : 100); // MySQL:
		// read
		// row-at-a-time.
		return this.statement.executeQuery();
	}
}
