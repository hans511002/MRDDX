package com.ery.hadoop.mrddx.db.mapreduce;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBInputFormat;
import com.ery.hadoop.mrddx.db.DBRecordReader;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * 
 * Copyrights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 
 *           从Oracle数据库中读取表记录的RecordReader对象。key类型是LongWritables,value类型是DBWritables
 *           。
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-7
 * 
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OracleDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {
	private static final Log LOG = LogFactory.getLog(OracleDBRecordReader.class);

	// 设置oracle时区的key值
	public static final String SESSION_TIMEZONE_KEY = "oracle.sessionTimeZone";

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
	 * @throws SQLException
	 */
	public OracleDBRecordReader(InputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
			DBConfiguration dbConfig, String cond, String[] fields, String table) throws SQLException {
		super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
		setSessionTimeZone(conf, conn);
	}

	@Override
	protected String getSelectQuery() {
		StringBuilder query = new StringBuilder();
		DBConfiguration dbConf = this.getDBConf();
		String tableName = this.getTableName();
		String[] fieldNames = this.getFieldNames();
		String conditions = this.getConditions();
		String orderByColumn = dbConf.getInputOrderByColumn();

		// Oracle-specific codepath to use rownum instead of LIMIT/OFFSET.
		if (dbConf.getInputQuery() == null) {
			query.append("SELECT ");
			if (fieldNames == null) {
				query.append(" * ");
			} else {
				for (int i = 0; i < fieldNames.length; i++) {
					query.append(fieldNames[i]);
					if (i != fieldNames.length - 1) {
						query.append(", ");
					}
				}
			}

			query.append(" FROM ").append(tableName);
			if (conditions != null && conditions.length() > 0) {
				query.append(" WHERE ").append(conditions);
			}

			if (orderByColumn != null && orderByColumn.length() > 0) {
				query.append(" ORDER BY ").append(orderByColumn);
			}
		} else {
			// PREBUILT QUERY
			query.append(dbConf.getInputQuery());
		}

		try {
			DBInputFormat.DBInputSplit split = this.getSplit();
			if (split.getLength() > 0 && split.getStart() >= 0) {
				String querystring = query.toString();
				query = new StringBuilder();
				query.append("SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( ");
				query.append(querystring);
				query.append(" ) a WHERE rownum <= ").append(split.getStart());
				query.append(" + ").append(split.getLength());
				query.append(" ) WHERE dbif_rno >= ").append(split.getStart() + 1);
			}
		} catch (Exception e) {
			// ignore, will not throw.
			MRLog.warnException(LOG, "Get the split length exception", e);
		}

		return query.toString();
	}

	/**
	 * 设置会话的时区
	 * 
	 * @param conf
	 *            配置对象. 用于读取 'oracle.sessionTimeZone' 属性
	 * @param conn
	 *            改变时区的连接属性
	 */
	public static void setSessionTimeZone(Configuration conf, Connection conn) throws SQLException {
		// 需要使用反射来调用该方法setSessionTimeZone OracleConnection类，因为Oracle特定的Java库
		// 在这种情况下无法访问。
		long logId = conf.getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		Method method = null;
		try {
			method = conn.getClass().getMethod("setSessionTimeZone", new Class[] { String.class });
		} catch (Exception e) {
			String message = "Could not find method setSessionTimeZone in " + conn.getClass().getName();
			MRLog.errorException(LOG, message, e);
			throw new SQLException(e);
		}

		// 需要设置时区，为了使Java正确地访问列"TIMESTAMP WITH LOCAL TIME ZONE"。
		// 从Java中，不能很容易地得到正确的Oracle特定时区的字符串 ，如果让用户在属性设置时区的话。
		String clientTimeZone = conf.get(SESSION_TIMEZONE_KEY, "GMT");
		try {
			method.setAccessible(true);
			method.invoke(conn, clientTimeZone);
			MRLog.info(LOG, logId, "Time zone has been set to " + clientTimeZone);
		} catch (Exception ex) {
			MRLog.warn(LOG, logId, "Time zone " + clientTimeZone + " could not be set on Oracle database.");
			MRLog.warn(LOG, logId, "Setting default time zone: GMT");
			try {
				// "GMT" timezone 一定存在
				method.invoke(conn, "GMT");
			} catch (Exception e) {
				MRLog.errorException(LOG, "Could not set time zone for oracle connection", e);
				throw new SQLException(e);
			}
		}
	}
}
