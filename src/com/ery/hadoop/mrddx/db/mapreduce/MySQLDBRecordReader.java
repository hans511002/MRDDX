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
import com.ery.hadoop.mrddx.db.DBRecordReader;

/**

 * 

 * @Comments 
 *           从MySQL数据库中读取表记录的RecordReader对象。key类型是LongWritables,value类型是DBWritables
 *           。

 * @version v1.0
 * @create Data 2013-1-7
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MySQLDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {
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
	public MySQLDBRecordReader(InputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
			DBConfiguration dbConfig, String cond, String[] fields, String table) {
		super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
	}

	@Override
	protected ResultSet executeQuery(DBInputFormat.DBInputSplit split) throws SQLException {
		this.statement = getConnection().prepareStatement(getSelectQuery(), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		// 注:缓冲设置过大，容易出现内存溢出.
		int fetchSize = this.dbConf.getInputQueryFetchSize();
		this.statement.setFetchSize(fetchSize > 0 ? fetchSize : 100); // read
		// row-at-a-time.Integer.MIN_VALUE
		return this.statement.executeQuery();
	}
}
