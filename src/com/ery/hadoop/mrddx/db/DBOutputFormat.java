package com.ery.hadoop.mrddx.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.DBMapper;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.vertica.VerticaOutputFormat;

/**

 * 

 * @Comments 输出到数据库的格式定义.

 * @version v1.0
 * @create Data 2013-1-7
 * 
 * @param <T>
 *            DBWritable
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DBOutputFormat<K extends DBWritable, V> extends OutputFormat<K, V> implements IHandleFormat {
	private static final Log LOG = LogFactory.getLog(DBOutputFormat.class);
	public static String DBRecordWriteClassName = "mr.output.dbrecord.writer.calss";
	protected Connection connection;
	protected Configuration conf = null;

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new NullOutputCommitter();
	}

	public void setDBRecordWriter(Configuration conf, Class<? extends DBRecordWriter> cla) {
		conf.set(DBRecordWriteClassName, cla.getName());
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		// public RecordWriter<K, NullWritable> getRecordWriter(FileSystem
		// ignored, JobConf job, String name,
		// Progressable progress) throws IOException {
		this.conf = context.getConfiguration();
		DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
		String tableName = dbConf.getOutputTableName();
		String[] fieldNames = dbConf.getOutputFieldNames();

		// 删除数据标示和主键字段
		boolean dataDeleteFlag = dbConf.getDbOutputDataDeleteFlag();
		String dataPrimarykeyField[] = dbConf.getDbOutputDataPrimarykeyField();

		try {
			Connection conn = this.getConnection(dbConf.getConf());
			long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
			String sql = "";
			if (dataDeleteFlag) {
				sql = constructDelete(tableName, dataPrimarykeyField);
				MRLog.consoleInfo(LOG, logId, "Delete SQL:" + sql);// 日志
			} else {
				sql = constructQuery(tableName, fieldNames);
				MRLog.consoleInfo(LOG, logId, "Insert SQL:" + sql);// 日志
			}

			PreparedStatement statement = conn.prepareStatement(sql);
			String DbRecordWr = dbConf.getConf().get(DBRecordWriteClassName, null);
			try {
				if (DbRecordWr != null) {
					Class<? extends DBRecordWriter> wr = (Class<? extends DBRecordWriter>) dbConf.getConf()
							.getClassByNameOrNull(DbRecordWr);
					return wr.getConstructor(conn.getClass(), statement.getClass(), dbConf.getClass()).newInstance(
							conn, statement, dbConf);
				} else {
					return new DBRecordWriter<K, V>(conn, statement, dbConf);
				}
			} catch (Exception e) {
				LOG.warn("构造特定DBRecordWriter【" + DbRecordWr + "】异常,使用默认的DBRecordWriter" + e.getMessage());
				return new DBRecordWriter<K, V>(conn, statement, dbConf);
			}
		} catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		this.conf = context.getConfiguration();
		DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
		String truncateSql = dbConf.getTruncateSql();
		if (truncateSql != null && !truncateSql.equals("")) {
			Statement stmt = null;
			Connection conn = getConnection(context.getConfiguration());
			try {
				stmt = conn.createStatement();
				long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
				MRLog.info(LOG, logId, truncateSql);
				stmt.execute(truncateSql);
			} catch (Exception e) {
			} finally {
				try {
					if (stmt != null)
						stmt.close();
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	public Connection getConnection(Configuration conf) throws IOException {
		try {
			if (connection == null) {
				DBConfiguration dbConf = new DBConfiguration(conf);
				connection = dbConf.getOutputConnection();
				connection.setAutoCommit(false);
				connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			}
			return connection;
		} catch (Exception e) {
			MRLog.errorException(LOG, "Create db connection exception.", e);
			throw new IOException(e);
		}
	}

	/**
	 * Constructs the query used as the prepared statement to insert data.
	 * 
	 * @param table
	 *            the table to insert into
	 * @param fieldNames
	 *            the fields to insert into. If field names are unknown, supply
	 *            an array of nulls.
	 */
	public String constructQuery(String table, String[] fieldNames) {
		if (fieldNames == null) {
			throw new IllegalArgumentException("Field names may not be null");
		}

		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO ").append(table);

		if (fieldNames.length > 0 && fieldNames[0] != null) {
			query.append(" (");
			for (int i = 0; i < fieldNames.length; i++) {
				// 支持调用函数转换数据后入表
				// 要求 output.sys.mr.mapred.field.names 和
				// output.sys.mr.mapred.group.field.method.rel 配置中的字段名称后增加
				// _method(函数名)method ，例如:字段名_method(字段名)method
				int Method_start = fieldNames[i].lastIndexOf("_method(");
				if (Method_start > 0) {
					query.append(fieldNames[i].substring(0, Method_start));
				} else
					query.append(fieldNames[i]);
				if (i != fieldNames.length - 1) {
					query.append(",");
				}
			}
			query.append(")");
		}
		query.append(" VALUES (");

		for (int i = 0; i < fieldNames.length; i++) {
			// 支持调用函数转换数据后入表
			String MethodName = "";
			int Method_start = fieldNames[i].lastIndexOf("_method(");
			if (Method_start > 0) {
				int Method_end = fieldNames[i].lastIndexOf(")method");
				MethodName = fieldNames[i].substring(Method_start + 8, Method_end);
			}
			if (!MethodName.equals("")) {
				query.append(MethodName + "(?)");
			} else
				query.append("?");
			if (i != fieldNames.length - 1) {
				query.append(",");
			}
		}
		query.append(")");

		return query.toString();
	}

	public String constructDelete(String table, String[] fieldNames) {
		if (fieldNames == null) {
			throw new IllegalArgumentException("Field names may not be null");
		}

		StringBuilder delete = new StringBuilder();
		delete.append("DELETE TABLE ").append(table);
		delete.append(" WHERE ");

		if (fieldNames.length > 0 && fieldNames[0] != null) {
			for (int i = 0; i < fieldNames.length; i++) {
				delete.append(fieldNames[i]);
				delete.append("=?");
				if (i != fieldNames.length - 1) {
					delete.append(" AND ");
				}
			}
		}

		return delete.toString();
	}

	/**
	 * Initializes the reduce-part of the job with the appropriate output
	 * settings
	 * 
	 * @param job
	 *            The job
	 * @param tableName
	 *            The table to insert data into
	 * @param fieldNames
	 *            The field names in the table.
	 * @throws SQLException
	 */
	public static void setOutput(Job job, String tableName, String[] fieldNames) throws IOException, SQLException {
		DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
		job.setOutputFormatClass(DBOutputFormat.class);
		job.setReduceSpeculativeExecution(false);
		dbConf.setOutputTableName(tableName);
		dbConf.setOutputFieldNames(fieldNames);

		// 获取表结构
		if (null == fieldNames || fieldNames.length <= 0) {
			try {
				fieldNames = dbConf.getOutputTableFieldNames("select * from " + tableName);
				dbConf.setOutputFieldNames(fieldNames);
				MRLog.info(LOG, "目标表的默认字段名：" + StringUtil.toString(fieldNames, null));
			} catch (Exception e) {
				MRLog.errorException(LOG, "获取目标表的默认字段名出错", e);
				throw new SQLException(e.getMessage());
			}
		}
	}

	public boolean isNeedCheckParams(String parName) {
		return true;
	}

	@Override
	public void handle(Job conf) throws Exception {
		this.conf = conf.getConfiguration();
		DBConfiguration dbconf = new DBConfiguration(conf.getConfiguration());
		// 表名
		String tableName = dbconf.getOutputTableName();
		if (isNeedCheckParams(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY) &&
				(null == tableName || tableName.length() <= 0)) {
			String meg = "目标表名<" + DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY + ">未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 数据库连接
		if (isNeedCheckParams(DBConfiguration.OUTPUT_URL_PROPERTY) &&
				!conf.getOutputFormatClass().equals(VerticaOutputFormat.class)) {
			String url = dbconf.getOutputJDBCUrl();
			if (null == url || url.length() <= 0) {
				String meg = "数据库地址<" + DBConfiguration.OUTPUT_URL_PROPERTY + ">未设置.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		// 如果输出字段未设置，这默认为表的所有字段
		String fieldNames[] = dbconf.getOutputFieldNames();
		if (null == fieldNames || fieldNames.length <= 0) {
			try {
				fieldNames = dbconf.getTableFieldNames("select * from " + tableName,
						this.getConnection(dbconf.getConf()));
				dbconf.setOutputFieldNames(fieldNames);
				MRLog.info(LOG, "目标表的默认字段名：" + StringUtil.toString(fieldNames, null));
			} catch (Exception e) {
				MRLog.errorException(LOG, "获取目标表的默认字段名出错", e);
				throw new SQLException(e.getMessage());
			}
		}

		// 不是数据库输入时检查输出字段是否存在
		if (!conf.getMapperClass().equals(DBMapper.class)) {
			boolean isExistFileId = false;
			for (int i = 0; i < fieldNames.length; i++) {
				if (DBConfiguration.DB_OUTPUT_DATA_FILE_FILED.equalsIgnoreCase(fieldNames[i])) {
					isExistFileId = true;
				}
			}
			if (isExistFileId) {
				String msg = "获取目标字段中不存在" + DBConfiguration.DB_OUTPUT_DATA_FILE_FILED + "字段";
				MRLog.error(LOG, msg);
				throw new Exception(msg);
			}
		}

		// 删除数据参数验证
		boolean dataDeleteFlag = dbconf.getDbOutputDataDeleteFlag();
		String dataPrimarykeyField[] = dbconf.getDbOutputDataPrimarykeyField();
		if (dataDeleteFlag) {
			if (null == dataPrimarykeyField || dataPrimarykeyField.length <= 0) {
				String meg = "设置删除标示符为true,<" + DBConfiguration.DB_OUTPUT_DATA_PRIMARYKEY_FIELD + ">必须设置.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		// 设置监听参数
		dbconf.setZKMonitor(true);

		conf.setOutputFormatClass(DBOutputFormat.class);
		conf.setReduceSpeculativeExecution(false);
		conf.setOutputKeyClass(DBRecord.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setReducerClass(DBReducer.class);
	}

}
