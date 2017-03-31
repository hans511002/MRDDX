package com.ery.hadoop.mrddx.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBMapper;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.db.mapreduce.MySQLDBRecordReader;
import com.ery.hadoop.mrddx.db.mapreduce.OracleDBRecordReader;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.vertica.VerticaInputFormat;
import com.ery.hadoop.mrddx.vertica.VerticaRecordReader;

/**

 * 

 * @Comments 从数据库读取数据的输入格式对象，是按照LIMIT和OFFSET进行拆分行数据的.

 * @version v1.0
 * @create Data 2013-1-7
 * 
 * @param <T>
 *            DBWritable
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DBInputFormat<T extends DBWritable> extends InputFormat<LongWritable, T> implements Configurable,
		IHandleFormat {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(DBInputFormat.class);

	// 数据库产品标识符
	protected String dbProductName = "DEFAULT";

	// 查询条件
	protected String conditions;

	// 数据库连接对象
	protected Connection connection;

	// 表名
	protected String tableName;

	// 查询的字段名称
	protected String[] fieldNames;

	// 配置对象
	protected Configuration conf;

	// 配置对象
	protected DBConfiguration dbConf;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		this.dbConf = new DBConfiguration(conf);
		this.tableName = this.dbConf.getInputTableName();
		this.fieldNames = this.dbConf.getInputFieldNames();
		this.conditions = this.dbConf.getInputConditions();

		try {
			System.out.println("inputClassName:" + this.getClass());
			getConnection();
			DatabaseMetaData dbMeta = this.connection.getMetaData();
			this.dbProductName = dbMeta.getDatabaseProductName().toUpperCase();

			long logId = dbConf.getJobLogId();
			MRLog.info(LOG, logId, "ProductName:" + this.dbProductName);
		} catch (Exception e) {
			MRLog.errorException(LOG, "Get database productName exception", e);
			throw new RuntimeException(e);
		}
	}

	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		Class<T> inputClass = (Class<T>) (this.dbConf.getInputClass());
		try {
			Connection conn = this.getConnection();
			// use database product name to determine appropriate record reader.
			if (this.dbProductName.startsWith("ORACLE")) {
				// use Oracle-specific db reader.
				MRLog.info(LOG, logId, "RecordReader:OracleDBRecordReader");
				return new OracleDBRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf, this.conditions,
						this.fieldNames, this.tableName);
			} else if (this.dbProductName.startsWith("MYSQL")) {
				// use MySQL-specific db reader.
				MRLog.info(LOG, logId, "RecordReader:MySQLDBRecordReader");
				return new MySQLDBRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf, this.conditions,
						this.fieldNames, this.tableName);
			} else if (VerticaInputFormat.class.equals(context.getInputFormatClass())) {
				MRLog.info(LOG, logId, "RecordReader:VerticaRecordReader");
				return new VerticaRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf, this.conditions,
						this.fieldNames, this.tableName);
			} else {
				// Generic reader.
				MRLog.info(LOG, logId, "RecordReader:DBRecordReader");
				return new DBRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf, this.conditions,
						this.fieldNames, this.tableName);
			}
		} catch (Exception e) {
			MRLog.errorException(LOG, "Get recordReader exception", e);
			throw new IOException(e.getMessage());
		}
	}

	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		ResultSet results = null;
		Statement statement = null;
		try {
			String countQuery = this.getCountQuery();
			statement = this.connection.createStatement();
			results = statement.executeQuery(countQuery);
			results.next();

			long count = results.getLong(1);// 记录总数
			int chunks = context.getConfiguration().getInt(MRConfiguration.MAPRED_MAP_TASKS, 1);
			long chunkSize = (count / chunks);// 单位记录数

			results.close();
			statement.close();

			// 行数拆分成n个的组块数，并相应地调整的最后一个块
			List<InputSplit> splits = new ArrayList<InputSplit>();
			for (int i = 0; i < chunks; i++) {
				DBInputSplit split = null;
				if ((i + 1) == chunks) {
					split = new DBInputSplit(i * chunkSize, count);
				} else {
					split = new DBInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
				}

				MRLog.info(LOG, "split:" + split.toString());
				splits.add(split);
			}

			MRLog.info(LOG, "Total # of splits" + splits.size());
			this.connection.commit();
			return splits;
		} catch (SQLException e) {
			MRLog.errorException(LOG, "Get splits exception", e);
			throw new IOException("Get splits exception", e);
		} finally {
			try {
				if (results != null) {
					results.close();
				}
			} catch (SQLException e1) {
			}

			try {
				if (statement != null) {
					statement.close();
				}
			} catch (SQLException e1) {
			}

			this.closeConnection();
		}
	}

	@Override
	public Configuration getConf() {
		return this.dbConf.getConf();
	}

	public DBConfiguration getDBConf() {
		return dbConf;
	}

	public Connection getConnection() throws Exception {
		try {
			if (null == this.connection) {
				// The connection was closed; reinstantiate it.
				this.connection = this.dbConf.getInputConnection();
				this.connection.setAutoCommit(false);
				this.connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			}
		} catch (Exception e) {
			MRLog.errorException(LOG, "Create db connection exception.", e);
			throw new Exception(e);
		}

		return this.connection;
	}

	public String getDBProductName() {
		return this.dbProductName;
	}

	/**
	 * 获取查询记录总数的sql语句
	 * 
	 * @return sql语句
	 */
	protected String getCountQuery() {
		// 1:若查询总数的sql语句不为null，则返回
		String countQuery = this.dbConf.getInputCountQuery();
		if (countQuery != null && countQuery.length() > 0) {
			MRLog.info(LOG, "CountQuery:" + countQuery);
			return countQuery;
		}

		// 2:组装查询总数的sql语句
		StringBuilder query = new StringBuilder();
		query.append("SELECT COUNT(*) FROM ");
		query.append(this.tableName);
		if (this.conditions != null && this.conditions.trim().length() > 0) {
			query.append(" WHERE ");
			query.append(this.conditions);
		}
		MRLog.info(LOG, "Default CountQuery:" + query.toString());
		return query.toString();
	}

	/**
	 * 初始化设置
	 * 
	 * @param job
	 *            The map-reduce job
	 * @param inputClass
	 *            实现DBWritable接口的记录对象
	 * @param inputQuery
	 *            查询记录, 例如:SELECT f1, f2, f3 FROM Mytable ORDER BY f1
	 * @param inputCountQuery
	 *            返回记录总数, 例如:SELECT COUNT(f1) FROM Mytable
	 */
	public static void setInput(Configuration job, Class<? extends DBWritable> inputClass, String inputQuery,
			String inputCountQuery, int queryfetchSize) {
		DBConfiguration dbConf = new DBConfiguration(job);
		dbConf.setInputClass(inputClass);
		dbConf.setInputQuery(inputQuery);
		dbConf.setInputCountQuery(inputCountQuery);
		dbConf.setInputQueryFetchSize(queryfetchSize);
	}

	/**
	 * 初始化设置
	 * 
	 * @param job
	 * @param tableName
	 *            表名
	 * @param fieldNames
	 *            源字段
	 * @param conditions
	 *            查询条件
	 * @param boundingQuery
	 *            绑定查询：必需设置查询最大最小值SQL
	 * @param orderBy
	 *            排序条件：使用数据驱动拆分时必需设置
	 * @throws SQLException
	 */
	public static void setInput(Configuration job, String tableName, String fieldNames, String conditions,
			String boundingQuery, String orderBy) throws SQLException {
		DBConfiguration dbconf = new DBConfiguration(job);
		dbconf.setInputTableName(tableName);
		dbconf.setInputFieldNames(fieldNames);
		dbconf.setInputConditions(conditions);
		dbconf.setInputBoundingQuery(boundingQuery);
		dbconf.setInputOrderByColumn(orderBy);

		if (!(null == fieldNames || fieldNames.trim().length() <= 0)) {
			return;
		}

		String querySQL = StringUtil.subSqlSelectToFrom(dbconf.getInputQuery());
		querySQL = null == querySQL ? ("select * from" + " " + tableName) : (querySQL + " " + tableName);

		// 从查询sql中获取输入字段
		try {
			fieldNames = StringUtil.toString(dbconf.getInputTableFieldNames(querySQL), ",");
			dbconf.setInputFieldNames(fieldNames);
			MRLog.info(LOG, "源表的默认字段名：" + fieldNames);
		} catch (Exception e) {
			MRLog.errorException(LOG, "获取源表的默认字段名出错", e);
			throw new SQLException(e.getMessage());
		}
	}

	/**
	 * 关闭连接
	 */
	protected void closeConnection() {
		try {
			if (null != this.connection) {
				this.connection.close();
				this.connection = null;
			}
		} catch (SQLException e) {
			MRLog.warnException(LOG, "Close db connection exception.", e);
		}
	}

	/**
	 * 


	 * 

	 * @Comments A InputSplit that spans a set of rows

	 * @version v1.0
	 * @create Data 2013-1-7
	 * 
	 */
	@InterfaceStability.Evolving
	public static class DBInputSplit extends InputSplit implements Writable {
		protected long end = 0;
		protected long start = 0;

		/**
		 * 默认构造方法
		 */
		public DBInputSplit() {
		}

		/**
		 * 构造方法
		 * 
		 * @param start
		 *            the index of the first row to select
		 * @param end
		 *            the index of the last row to select
		 */
		public DBInputSplit(long start, long end) {
			this.start = start;
			this.end = end;
		}

		@Override
		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		/**
		 * The index of the first row to select
		 * 
		 * @return
		 */
		public long getStart() {
			return start;
		}

		/**
		 * The index of the last row to select
		 * 
		 * @return
		 */
		public long getEnd() {
			return end;
		}

		@Override
		public long getLength() {
			return this.end - this.start;// The total row count in this split
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			this.start = input.readLong();
			this.end = input.readLong();
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeLong(this.start);
			output.writeLong(this.end);
		}

		@Override
		public String toString() {
			return "start:" + this.start + " end:" + this.end;
		}
	}

	/**
	 * 


	 * 

	 * @Comments A Class that does nothing, implementing DBWritable

	 * @version v1.0
	 * @create Data 2013-1-7
	 * 
	 */
	@InterfaceStability.Evolving
	public static class NullDBWritable implements DBWritable, Writable {
		@Override
		public void readFields(DataInput in) throws IOException {
		}

		@Override
		public void readFields(ResultSet arg0, String[] arg1) throws SQLException {
		}

		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public void write(PreparedStatement arg0) throws SQLException {
		}

		@Override
		public void write(PreparedStatement statement, String[] fieldNames) throws SQLException {
		}

		@Override
		public String[] getFieldNames() {
			return null;
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
		}
	}

	@Override
	public void handle(Job conf) throws Exception {
		DBConfiguration dbconf = new DBConfiguration(conf.getConfiguration());
		System.out.println("InputFormatClass=" + conf.getInputFormatClass());
		if (VerticaInputFormat.class.equals(conf.getInputFormatClass())) {

		} else {
			// 数据库地址
			String url = dbconf.getInputJDBCUrl();
			if (null == url || url.trim().length() <= 0) {
				String meg = "数据库地址<" + DBConfiguration.INPUT_URL_PROPERTY + ">未设置.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			// 数据库用户名
			String userName = dbconf.getInputJDBCUserName();
			if (null == userName || userName.trim().length() <= 0) {
				String meg = "数据库用户名<" + DBConfiguration.INPUT_USERNAME_PROPERTY + ">未设置.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			// 数据库密码
			String password = dbconf.getInputJDBCPassword();
			if (null == password || password.trim().length() <= 0) {
				String meg = "数据库密码<" + DBConfiguration.INPUT_PASSWORD_PROPERTY + ">未设置.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		// 查询缓冲区大小
		int fetchSize = dbconf.getInputQueryFetchSize();
		if (fetchSize <= 0) {
			String meg = "查询缓冲区大小<" + DBConfiguration.INPUT_QUERY_FETCHSIZE + ">必须大于0.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 当未设置输入字段时，通过查询语句获取字段
		String[] fieldNames = dbconf.getInputFieldNames();
		if (null == fieldNames || fieldNames.length <= 0) {
			String tName = dbconf.getInputTableName();
			if (null == tName || tName.trim().length() <= 0) {
				String meg = "输入字段未设置时，表名<" + DBConfiguration.INPUT_TABLE_NAME_PROPERTY + ">必须设置.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			String querySQL = StringUtil.subSqlSelectToFrom(dbconf.getInputQuery());
			querySQL = null == querySQL ? ("select * from" + " " + tName) : (querySQL + " " + tName);

			// 从查询sql中获取输入字段
			try {
				String fields = StringUtil.toString(dbconf.getInputTableFieldNames(querySQL), ",");
				dbconf.setInputFieldNames(fields);
				MRLog.info(LOG, "源表的默认字段名：" + fields);
			} catch (Exception e) {
				MRLog.errorException(LOG, "获取源表的默认字段名出错", e);
				throw new SQLException(e.getMessage());
			}
		}

		// 验证参数
		if (!VerticaInputFormat.class.equals(conf.getInputFormatClass()))
			this.validateInputParameter(dbconf);

		// conf.setMapperClass();
		conf.setMapperClass(DBMapper.class);
		conf.setMapOutputKeyClass(DBRecord.class);
		conf.setMapOutputValueClass(DBRecord.class);
		if (dbconf.getInputIsCombiner()) {
			conf.setCombinerClass(DBGroupReducer.class);
			// conf.setCombinerKeyGroupingComparatorClass(DBRecord.class);
		}
		dbconf.setInputClass(DBRecord.class);
		conf.setInputFormatClass(DBInputFormat.class);
	}

	/**
	 * 源数据是数据库表，输入参数验证
	 * 
	 * @param dbconf
	 * @throws Exception
	 */
	protected void validateInputParameter(DBConfiguration dbconf) throws Exception {
		String query = dbconf.getInputQuery();
		String countQuery = dbconf.getInputCountQuery();
		String tableName = dbconf.getInputTableName();
		String fieldNames[] = dbconf.getInputFieldNames();
		String conditions = dbconf.getInputConditions();
		String orderBy = dbconf.getInputOrderByColumn();

		boolean isNullpInputQuery = (null == query || query.trim().length() <= 0);
		boolean isNullpInputCountQuery = (null == countQuery || countQuery.trim().length() <= 0);
		boolean isNullpInputTableName = (null == tableName || tableName.trim().length() <= 0);
		boolean isNullpInputFieldNames = (null == fieldNames || fieldNames.length <= 0);
		boolean isNullpInputConditions = (null == conditions || conditions.trim().length() <= 0);
		boolean isNullpInputOrderByColumn = (null == orderBy || orderBy.trim().length() <= 0);

		// 通过拆分类型验证
		String meg = "";
		// A.参数设置(设置：inputQuery,inputCountQuery,可选inputFieldNames和inputTableName至少设置一个)
		if (!isNullpInputQuery && !isNullpInputCountQuery) {
			if (!isNullpInputFieldNames) {
				return;
			}

			if (isNullpInputFieldNames && isNullpInputTableName) {
				meg = "源字段<inputFieldNames>未设置的情况下，表名<inputTableName>不能为空.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			return;
		}

		// B.参数设置(设置必选：inputQuery,inputTableName,可选:inputFieldNames,inputConditions)
		if (!isNullpInputQuery && !isNullpInputTableName) {
			if (isNullpInputFieldNames) {
				MRLog.warn(LOG, "源字段<inputFieldNames>未设置，默认为<inputQuery>查询参数的字段.");
			}

			if (isNullpInputConditions) {
				MRLog.warn(LOG, "查询条件<inputConditions>未设置,默认为空.");
			}
			return;
		}

		// c.参数设置(设置必选：inputCountQuery,inputTableName,可选:inputFieldNames,inputConditions,inputOrderBy)
		if (!isNullpInputCountQuery && !isNullpInputTableName) {
			if (isNullpInputFieldNames) {
				meg = "源字段<inputFieldNames>未设置，默认为<inputTableName>表的所有字段.";
				MRLog.warn(LOG, meg);
			}

			if (isNullpInputConditions) {
				MRLog.warn(LOG, "查询条件<inputConditions>未设置,默认为空.");
			}

			if (isNullpInputOrderByColumn) {
				MRLog.warn(LOG, "排序条件<inputOrderByColumn>未设置,默认为空.");
			}
			return;
		}

		// d.参数设置(设置：inputTableName,可选:inputFieldNames,inputConditions,inputOrderBy)
		if (!isNullpInputTableName) {
			if (isNullpInputFieldNames) {
				meg = "源字段<inputFieldNames>未设置，默认为<inputTableName>表的所有字段.";
				MRLog.warn(LOG, meg);
			}

			if (isNullpInputConditions) {
				MRLog.warn(LOG, "查询条件<inputConditions>未设置,默认为空.");
			}

			if (isNullpInputOrderByColumn) {
				MRLog.warn(LOG, "排序条件<inputOrderByColumn>未设置,默认为空.");
			}
			return;
		}

		meg = "按行拆分类型的方式，输入参数情况都不满足.";
		MRLog.error(LOG, meg);
		throw new Exception(meg);
	}
}
