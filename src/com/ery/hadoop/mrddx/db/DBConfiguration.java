package com.ery.hadoop.mrddx.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * A container for configuration property names for jobs with DB input/output.
 * The job can be configured using the static methods in this class,
 * {@link DBInputFormat}, and {@link DBOutputFormat}. Alternatively, the
 * properties can be set in the configuration with proper values.
 * 
 * @see DBConfiguration#configureDB(Configuration, String, String, String,
 *      String)
 * @see DBInputFormat#setInput(Job, Class, String, String)
 * @see DBInputFormat#setInput(Job, Class, String, String, String, String...)
 * @see DBOutputFormat#setOutput(Job, String, String...)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DBConfiguration extends MRConfiguration {
	private static final Log LOG = LogFactory.getLog(DBConfiguration.class);
	private static final String MYSQL_Driver = "org.gjt.mm.mysql.Driver";
	private static final String ORACEL_Driver = "oracle.jdbc.driver.OracleDriver";
	private static final String COBAR_Driver = "com.alibaba.cobar.jdbc.Driver";
	public static final String RDB_DATA_SOURCE_DRIVER = "rdb.data.source.driver";
	private static final String PostgreSQL_Driver = "org.postgresql.Driver";

	/**
	 * JDBC(input)
	 */
	// JDBC Database access URL
	public static final String INPUT_URL_PROPERTY = "input.mr.mapred.jdbc.url";
	// User name to access the database
	public static final String INPUT_USERNAME_PROPERTY = "input.mr.mapred.jdbc.username";
	// Password to access the database
	public static final String INPUT_PASSWORD_PROPERTY = "input.mr.mapred.jdbc.password";
	// Input table name
	public static final String INPUT_TABLE_NAME_PROPERTY = "input.mr.mapred.jdbc.table.name";
	// WHERE clause in the input SELECT statement
	public static final String INPUT_CONDITIONS_PROPERTY = "input.mr.mapred.jdbc.conditions";
	// ORDER BY clause in the input SELECT statement
	public static final String INPUT_ORDER_BY_PROPERTY = "input.mr.mapred.jdbc.orderby";
	// Whole input query, exluding LIMIT...OFFSET
	public static final String INPUT_QUERY = "input.mr.mapred.jdbc.query";
	// Input query to get the count of records
	public static final String INPUT_COUNT_QUERY = "input.mr.mapred.jdbc.count.query";
	// Input query FetchSize
	public static final String INPUT_QUERY_FETCHSIZE = "input.mr.mapred.jdbc.query.fetchsize";
	// Input query to get the max and min values of the jdbc.input.query
	public static final String INPUT_BOUNDING_QUERY = "input.mr.mapred.jdbc.bounding.query";
	// Input query to get the partions values of the jdbc.input.query
	public static final String INPUT_PARTITION_QUERY = "input.mr.mapred.jdbc.partition.query";
	public static final String INPUT_QUERY_PARAM_ISMACROS = "input.mr.mapred.query.param.ismacros";

	/**
	 * JDBC(output)
	 */
	// JDBC Database access URL
	public static final String OUTPUT_URL_PROPERTY = "output.mr.mapred.jdbc.url";
	// User name to access the database
	public static final String OUTPUT_USERNAME_PROPERTY = "output.mr.mapred.jdbc.username";
	// Password to access the database
	public static final String OUTPUT_PASSWORD_PROPERTY = "output.mr.mapred.jdbc.password";
	// Output table name
	public static final String OUTPUT_TABLE_NAME_PROPERTY = "output.mr.mapred.jdbc.table.name";
	// Number of fields in the Output table
	public static final String DB_OUTPUT_BUFFER_LENGTH = "output.mr.mapred.jdbc.buffer.length";
	// delete data flag(true:delete), default:false
	public static final String DB_OUTPUT_DATA_DELETE_FLAG = "output.mr.mapred.jdbc.data.delete.flag";
	// delete data fields example==> field1,field2..
	public static final String DB_OUTPUT_DATA_PRIMARYKEY_FIELD = "output.mr.mapred.jdbc.data.primarykey.fields";
	// Output field must exist: MRFILE_ID (file ID)
	public static final String DB_OUTPUT_DATA_FILE_FILED = "MRFILE_ID";
	public static final String DB_OUTPUT_TRUNCATE_SQL = "output.mr.mapred.jdbc.table.dropsql";
	public static final String DB_VERTICA_IS_OPTIMIZE_OUTPUT_TABLE = "output.mr.mapred.is.optimize.table";

	public DBConfiguration(Configuration job) {
		super(job);
		this.conf = job;
	}

	/**
	 * Sets the DB access related fields in the {@link Configuration}.
	 * 
	 * @param conf
	 *            the configuration
	 * @param driverClass
	 *            JDBC Driver class name
	 * @param dbUrl
	 *            JDBC DB access URL.
	 * @param userName
	 *            DB access username
	 * @param passwd
	 *            DB access passwd
	 */
	public static void configureInputDB(Configuration conf, String driverClass, String dbUrl, String userName,
			String passwd) {
		conf.set(INPUT_URL_PROPERTY, dbUrl);
		if (userName != null) {
			conf.set(INPUT_USERNAME_PROPERTY, userName);
		}
		if (passwd != null) {
			conf.set(INPUT_PASSWORD_PROPERTY, passwd);
		}
	}

	/**
	 * Sets the DB access related fields in the JobConf.
	 * 
	 * @param job
	 *            the job
	 * @param driverClass
	 *            JDBC Driver class name
	 * @param dbUrl
	 *            JDBC DB access URL.
	 */
	public static void configureInputDB(Configuration job, String driverClass, String dbUrl) {
		configureInputDB(job, driverClass, dbUrl, null, null);
	}

	public static void configureOutputDB(Configuration job, String driverClass, String dbUrl) {
		configureOutputDB(job, driverClass, dbUrl, null, null);
	}

	public static void configureOutputDB(Configuration conf, String driverClass, String dbUrl, String userName,
			String passwd) {
		conf.set(OUTPUT_URL_PROPERTY, dbUrl);
		if (userName != null) {
			conf.set(OUTPUT_USERNAME_PROPERTY, userName);
		}
		if (passwd != null) {
			conf.set(OUTPUT_PASSWORD_PROPERTY, passwd);
		}
	}

	public Configuration getConf() {
		return conf;
	}

	public String getTruncateSql() {
		return conf.get(DB_OUTPUT_TRUNCATE_SQL, "");
	}

	public void setTruncateSql(String sql) {
		if (sql != null && sql.length() > 0) {
			conf.set(DBConfiguration.DB_OUTPUT_TRUNCATE_SQL, sql);
		}
	}

	// JDBC(input)
	/**
	 * 获取JDBC的连接地址
	 */
	public String getInputJDBCUrl() {
		return conf.get(INPUT_URL_PROPERTY, "");
	}

	/**
	 * 获取JDBC的用户名
	 */
	public String getInputJDBCUserName() {
		return conf.get(INPUT_USERNAME_PROPERTY, "");
	}

	/**
	 * 获取JDBC的用户名
	 */
	public String getInputJDBCPassword() {
		return conf.get(INPUT_PASSWORD_PROPERTY, "");
	}

	/**
	 * 获取查询sql语句
	 * 
	 * @return
	 */
	public String getInputQuery() {
		return conf.get(DBConfiguration.INPUT_QUERY);
	}

	public void setInputQuery(String query) {
		if (query != null && query.length() > 0) {
			conf.set(DBConfiguration.INPUT_QUERY, query);
		}
	}

	/**
	 * 获取查询总数的sql语句
	 * 
	 * @return
	 */
	public String getInputCountQuery() {
		return conf.get(DBConfiguration.INPUT_COUNT_QUERY, null);
	}

	public void setInputCountQuery(String query) {
		if (query != null && query.length() > 0) {
			conf.set(DBConfiguration.INPUT_COUNT_QUERY, query);
		}
	}

	/**
	 * 获取查询缓冲区大小
	 * 
	 * @return
	 */
	public int getInputQueryFetchSize() {
		return conf.getInt(DBConfiguration.INPUT_QUERY_FETCHSIZE, 100);
	}

	public void setInputQueryFetchSize(int fetchSize) {
		conf.setInt(DBConfiguration.INPUT_QUERY_FETCHSIZE, fetchSize);
	}

	/**
	 * 获取表名
	 * 
	 * @return
	 */
	public String getInputTableName() {
		return conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "");
	}

	public void setInputTableName(String tableName) {
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
	}

	/**
	 * 获取查询条件
	 * 
	 * @return
	 */
	public String getInputConditions() {
		return conf.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY, null);
	}

	public void setInputConditions(String conditions) {
		if (conditions != null && conditions.length() > 0)
			conf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, conditions);
	}

	/**
	 * Set the user-defined bounding query to use with a user-defined query.
	 * This *must* include the substring "$CONDITIONS"
	 * (DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) inside the WHERE clause, so
	 * that DataDrivenDBInputFormat knows where to insert split clauses. e.g.,
	 * "SELECT foo FROM mytable WHERE $CONDITIONS and name like ''" This will be
	 * expanded to something like: SELECT foo FROM mytable WHERE (id &gt; 100)
	 * AND (id &lt; 250) inside each split.
	 */
	public void setInputBoundingQuery(String query) {
		if (query != null && query.length() > 0) {
			conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
		}
	}

	public String getInputBoundingQuery() {
		return conf.get(DBConfiguration.INPUT_BOUNDING_QUERY, null);
	}

	public void setInputPartitionQuery(String query) {
		if (query != null && query.length() > 0) {
			conf.set(DBConfiguration.INPUT_PARTITION_QUERY, query);
		}
	}

	public String getInputPartitionQuery() {
		return conf.get(DBConfiguration.INPUT_PARTITION_QUERY, null);
	}

	/**
	 * 获取排序条件
	 * 
	 * @return
	 */
	public String getInputOrderByColumn() {
		return conf.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY);
	}

	public void setInputOrderByColumn(String orderby) {
		if (orderby != null && orderby.length() > 0) {
			conf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby);
		}
	}

	// JDBC(output)
	/**
	 * 获取输出JDBC的连接地址
	 */
	public String getOutputJDBCUrl() {
		return conf.get(OUTPUT_URL_PROPERTY, "");
	}

	/**
	 * 获取输出JDBC的用户名
	 */
	public String getOutputJDBCUserName() {
		return conf.get(OUTPUT_USERNAME_PROPERTY, "");
	}

	/**
	 * 获取输出JDBC的密码
	 */
	public String getOutputJDBCPassword() {
		return conf.get(OUTPUT_PASSWORD_PROPERTY, "");
	}

	/**
	 * 获取输出表名
	 */
	public String getOutputTableName() {
		return conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
	}

	public void setOutputTableName(String tableName) {
		conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
	}

	/**
	 * 输出submit的缓冲数
	 * 
	 * @param def
	 * @return
	 */
	public int getDbOutputBufferLength(int def) {
		return conf.getInt(DB_OUTPUT_BUFFER_LENGTH, def);
	}

	public void setDbOutputBufferLength(int len) {
		conf.setInt(DB_OUTPUT_BUFFER_LENGTH, len);
	}

	public boolean getDbOutputDataDeleteFlag() {
		return conf.getBoolean(DB_OUTPUT_DATA_DELETE_FLAG, false);
	}

	public void setDbOutputDataDeleteFlag(boolean flag) {
		conf.setBoolean(DB_OUTPUT_DATA_DELETE_FLAG, flag);
	}

	public String[] getDbOutputDataPrimarykeyField() {
		return conf.getStrings(DB_OUTPUT_DATA_PRIMARYKEY_FIELD, new String[0]);
	}

	public void setDbOutputDataPrimarykeyField(String fields) {
		conf.set(DB_OUTPUT_DATA_PRIMARYKEY_FIELD, fields);
	}

	/**
	 * 获取数据库的名称
	 * 
	 * @param conn
	 * @return
	 */
	public String getDBProduceName(Connection conn) {
		if (null == conn) {
			return null;
		}

		try {
			DatabaseMetaData dbMeta = conn.getMetaData();
			return dbMeta.getDatabaseProductName().toUpperCase();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (null != conn) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return null;
	}

	/**
	 * 获取源数据表的所有字段名
	 * 
	 * @param sql
	 * @param conn
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public String[] getInputTableFieldNames(String sql) throws SQLException, ClassNotFoundException {
		return this.getTableFieldNames(sql, this.getInputConnection());
	}

	public Connection getOutputConnection() throws ClassNotFoundException, SQLException {
		String url = this.getOutputJDBCUrl();
		String driverName = this.getDriverName(url, false);
		String user = this.getOutputJDBCUserName();
		String pwd = this.getOutputJDBCPassword();
		if (null == driverName) {
			throw new SQLException("连接地址不正确,url:" + url + " driverName:" + driverName + " user:" + user + " pwd:" + pwd);
		}

		Class.forName(driverName);
		if (null == user) {
			return DriverManager.getConnection(url);
		} else {
			return DriverManager.getConnection(url, user, pwd);
		}
	}

	/**
	 * 获取目标表的所有字段名
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public String[] getOutputTableFieldNames(String sql) throws SQLException, ClassNotFoundException {
		return this.getTableFieldNames(sql, this.getOutputConnection());
	}

	/**
	 * 获取表的所有字段名
	 * 
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public String[] getTableFieldNames(String sql, Connection conn) throws SQLException {
		if (null == conn) {
			throw new SQLException("GET table fieldNames Connection error");
		}

		MRLog.info(LOG, "Get query table'field sql:" + sql);

		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			String[] fieldNames = new String[columnCount];
			for (int i = 0; i < columnCount; i++) {
				fieldNames[i] = rsmd.getColumnName(i + 1); // 列名
			}
			return fieldNames;
		} catch (SQLException e) {
			throw new SQLException(StringUtil.stringifyException(e));
		} finally {
			close(conn);// 关闭连接
		}
	}

	/**
	 * 关闭连接
	 * 
	 * @param conn
	 */
	public void close(Connection conn) {
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				MRLog.error(LOG, "Close connection error!");
			}
		}
	}

	/**
	 * Returns a connection object o the DB
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Connection getInputConnection(String driver, String url, String userName, String passwd)
			throws ClassNotFoundException, SQLException {
		Class.forName(driver);
		if (userName == null) {
			return DriverManager.getConnection(url);
		} else {
			return DriverManager.getConnection(url, userName, passwd);
		}
	}

	/**
	 * Returns a connection object o the DB
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Connection getInputConnection() throws ClassNotFoundException, SQLException {
		String url = this.getInputJDBCUrl();
		String driverName = this.getDriverName(url, true);
		String user = this.getInputJDBCUserName();
		String pwd = this.getInputJDBCPassword();
		if (null == driverName) {
			throw new SQLException("连接地址不正确,url:" + url + " driverName:" + driverName + " user:" + user + " pwd:" + pwd);
		}

		Class.forName(driverName);
		if (null == user) {
			return DriverManager.getConnection(url);
		} else {
			return DriverManager.getConnection(url, user, pwd);
		}
	}

	/**
	 * 获取driver 名称
	 * 
	 * @param dburl
	 *            连接地址
	 * @return
	 */
	public String getDriverName(String dburl, boolean isInput) {
		if (null == dburl || dburl.trim().length() <= 0) {
			return null;
		}
		if (dburl.startsWith("jdbc:mysql")) {
			return MYSQL_Driver;
		} else if (dburl.startsWith("jdbc:oracle")) {
			return ORACEL_Driver;
		} else if (dburl.startsWith("jdbc:cobar")) {
			return COBAR_Driver;
		} else if (dburl.startsWith("jdbc:postgresql")) {
			return PostgreSQL_Driver;
		} else {
			return this.conf.get((isInput ? "input." : "output.") + RDB_DATA_SOURCE_DRIVER, null);
		}
	}

}
