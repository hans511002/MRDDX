package com.ery.hadoop.mrddx.hive;

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
import com.ery.hadoop.mrddx.hive.HiveInputFormat.HiveInputSplit;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * Hive读取记录
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-19
 * @version v1.0
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveRecordReader<T extends DBWritable> extends RecordReader<LongWritable, T> {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(HiveRecordReader.class);

	// 结果集
	private ResultSet results;

	// 输入类型
	private Class<T> inputClass;

	// 配置对象
	private Configuration conf;

	// Hive的拆分对象
	private HiveInputSplit split;

	// 当前位置
	private long pos = 0;

	// key
	private LongWritable key;

	// value
	private T value;

	// 连接对象
	private Connection connection;

	// statement对象
	protected PreparedStatement statement;

	// 字段列表
	private String[] fieldNames;

	// 查询sql语句
	private String querySQL;

	/**
	 * 构造方法
	 * 
	 * @param split 拆分对象
	 * @param inputClass value的class类型
	 * @param conf 系统配置
	 * @param conn 连接对象
	 * @param fieldNames 字段名称数组
	 */
	public HiveRecordReader(HiveInputFormat.HiveInputSplit split, Class<T> inputClass, Configuration conf,
			Connection conn, String querySql, String[] fieldNames) {
		this.inputClass = inputClass;
		this.split = split;
		this.conf = conf;
		this.connection = conn;
		this.querySQL = querySql;
		this.fieldNames = fieldNames;
	}

	/**
	 * 执行查询
	 * 
	 * @return 查询结果
	 * @throws SQLException SQL异常
	 */
	protected ResultSet executeQuery(String query) throws SQLException {
		this.statement = this.connection.prepareStatement(query);
		return this.statement.executeQuery();
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
			LOG.error("Close connection error!");
			throw new IOException(e.getMessage());
		}
	}

	public LongWritable getCurrentKey() {
		return this.key;
	}

	public T getCurrentValue() {
		return this.value;
	}

	public LongWritable createKey() {
		return new LongWritable(0);
	}

	public T createValue() {
		return ReflectionUtils.newInstance(this.inputClass, this.conf);
	}

	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public float getProgress() throws IOException {
		return this.pos / (float) this.split.getLength();
	}

	public boolean nextKeyValue() throws IOException {
		try {
			if (null == this.key) {
				this.key = new LongWritable();
			}
			if (null == this.value) {
				this.value = createValue();
			}
			if (null == this.results) {
				// 执行查询语句
				MRLog.consoleDebug(LOG, "Hive的查询SQL=" + querySQL + " 结果如下:");
				this.results = this.executeQuery(querySQL);
			}
			if (!this.results.next()) {
				return false;
			}

			// Set the key field value as the output key value
			this.key.set(this.pos + this.split.getStart());
			if (this.fieldNames == null) {
				ResultSetMetaData RsMd = this.results.getMetaData();
				int ColsCount = RsMd.getColumnCount();
				this.fieldNames = new String[ColsCount];
				for (int n = 0; n < ColsCount; n++) {
					this.fieldNames[n] = RsMd.getColumnName(n);
				}
			}

			this.value.readFields(this.results, this.fieldNames);
			this.pos++;
			if (this.pos % 10000 == 0) {
				MRLog.consoleDebug(LOG, "read " + this.pos + " rows");
			}
		} catch (SQLException e) {
			LOG.error("SQLException in nextKeyValue");
			throw new IOException("SQLException in nextKeyValue", e);
		}

		return true;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
	}
}
