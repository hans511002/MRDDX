package com.ery.hadoop.mrddx.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * 

 * 

 * @Comments 从表中读取记录的RecordReader对象。key类型是LongWritables,value类型是DBWritables。

 * @version v1.0
 * @create Data 2013-1-7
 * 
 * @param <T>
 *            DBWritable
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DBRecordReader<T extends DBWritable> extends RecordReader<LongWritable, T> {
	// 日志对象
	protected static final Log LOG = LogFactory.getLog(DBRecordReader.class);

	// 拆分对象
	protected DBInputFormat.DBInputSplit split;

	// 记录类型
	protected Class<T> inputClass;

	// 配置对象
	protected Configuration conf;

	// 数据库连接对象
	protected Connection connection;

	// 配置对象
	protected DBConfiguration dbConf;

	// 查询条件表达式
	protected String conditions;

	// 查询字段名称
	protected String[] fieldNames;

	// 表名
	protected String tableName;

	// 结果集对象
	protected ResultSet results;

	// 当前位置
	protected long pos = 0;

	// key对象
	protected LongWritable key;

	// value对象
	protected T value;

	// statement对象
	protected PreparedStatement statement;

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
	 * @param conditions
	 *            查询条件表达式
	 * @param fields
	 *            查询字段名称
	 * @param table
	 *            表名
	 * @throws IOException
	 *             IO异常
	 */
	public DBRecordReader(InputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
			DBConfiguration dbConfig, String conditions, String[] fields, String table) {
		if (split instanceof DBInputFormat.DBInputSplit) {
			this.split = (DBInputFormat.DBInputSplit) split;
		} else {
			String message = "split not instanceof DBInputFormat.DBInputSplit";
			MRLog.error(LOG, message);
		}

		this.inputClass = inputClass;
		this.conf = conf;
		this.connection = conn;
		this.dbConf = dbConfig;
		this.conditions = conditions;
		this.fieldNames = fields;
		this.tableName = table;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
	}

	/**
	 * 获取next记录
	 * 
	 * @return 是否还有下一条数据
	 * @throws IOException
	 */
	public boolean nextKeyValue() throws IOException {
		try {
			if (null == this.key) {
				this.key = new LongWritable();
			}

			if (null == this.value) {
				this.value = createValue();
			}

			if (null == this.results) {
				// First time into this method, run the query.
				MRLog.consoleDebug(LOG, "Query SQL:" + getSelectQuery());
				this.results = this.executeQuery(this.split);
			}

			if (!this.results.next()) {
				return false;
			}

			// Set the key field value as the output key value
			this.key.set(this.pos + this.split.getStart());
			if (null == this.fieldNames) {
				ResultSetMetaData rsMd = this.results.getMetaData();
				int ColsCount = rsMd.getColumnCount();
				this.fieldNames = new String[ColsCount];
				for (int n = 0; n < ColsCount; n++) {
					this.fieldNames[n] = rsMd.getColumnName(n);
				}
			}

			this.value.readFields(this.results, this.fieldNames);
			this.pos++;
			if (this.pos % 10000 == 0) {
				MRLog.consoleDebug(LOG, "Read " + this.pos + " Rows");
			}
		} catch (SQLException e) {
			MRLog.errorException(LOG, "SQLException in nextKeyValue", e);
			throw new IOException("SQLException in nextKeyValue", e);
		}

		return true;
	}

	/**
	 * 返回查询sql
	 * 
	 * @return 查询sql
	 */
	protected String getSelectQuery() {
		StringBuilder query = new StringBuilder();

		// Default codepath for MySQL, HSQLDB, etc. Relies on LIMIT/OFFSET for
		// splits.
		if (this.dbConf.getInputQuery() == null) {
			query.append("SELECT ");
			if (null == this.fieldNames) {
				query.append(" * ");
			} else {
				for (int i = 0; i < this.fieldNames.length; i++) {
					query.append(this.fieldNames[i]);
					if (i != this.fieldNames.length - 1) {
						query.append(", ");
					}
				}
			}

			query.append(" FROM ").append(this.tableName);
			query.append(" AS ").append(this.tableName); // in hsqldb this is
			// necessary
			if (this.conditions != null && this.conditions.length() > 0) {
				query.append(" WHERE (").append(this.conditions).append(")");
			}

			String orderByColumn = this.dbConf.getInputOrderByColumn();
			if (orderByColumn != null && orderByColumn.length() > 0) {
				query.append(" ORDER BY ").append(orderByColumn);
			}
		} else {
			// PREBUILT QUERY
			query.append(this.dbConf.getInputQuery());
		}

		try {
			query.append(" LIMIT ").append(this.split.getLength());
			query.append(" OFFSET ").append(this.split.getStart());
		} catch (Exception e) {
			// Ignore, will not throw.
			MRLog.warnException(LOG, "Get the split length exception", e);
		}

		return query.toString();
	}

	/**
	 * 执行查询语句，返回结果集
	 * 
	 * @param query
	 *            查询语句
	 * @return 返回结果集
	 * @throws SQLException
	 *             SQL异常
	 */
	protected ResultSet executeQuery(DBInputFormat.DBInputSplit split) throws SQLException {
		this.statement = this.connection.prepareStatement(getSelectQuery(), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		// 注:缓冲设置过大，容易出现内存溢出.
		int fetchSize = this.dbConf.getInputQueryFetchSize();
		this.statement.setFetchSize(fetchSize > 0 ? fetchSize : 100); // read
		return statement.executeQuery();
	}

	public LongWritable createKey() {
		return new LongWritable(0);
	}

	public T createValue() {
		return ReflectionUtils.newInstance(this.inputClass, this.conf);
	}

	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return this.pos / (float) this.split.getLength();
	}

	@Override
	public void close() throws IOException {
		MRLog.consoleDebug(LOG, "Total: read " + this.pos + " rows");
		try {
			if (null != this.results) {
				this.results.close();
			}
			if (null != this.statement) {
				this.statement.close();
			}
			if (null != this.connection) {
				this.connection.close();
			}
		} catch (SQLException e) {
			throw new IOException(e.getMessage());
		}
	}

	protected DBInputFormat.DBInputSplit getSplit() {
		return split;
	}

	protected String[] getFieldNames() {
		return fieldNames;
	}

	protected String getTableName() {
		return tableName;
	}

	protected String getConditions() {
		return conditions;
	}

	protected DBConfiguration getDBConf() {
		return dbConf;
	}

	protected Connection getConnection() {
		return connection;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public T getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}
}
