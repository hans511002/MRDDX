package com.ery.hadoop.mrddx.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * A RecordWriter that writes the reduce output to a SQL table
 * 



 * @create Data 2013-1-7
 * @version v1.0
 * @param <K>
 * @param <V>
 */
@InterfaceStability.Evolving
public class DBRecordWriter<K extends DBWritable, V> extends RecordWriter<K, V> {
	// 日志对象
	protected static final Log LOG = LogFactory.getLog(DBRecordWriter.class);

	// 连接对象
	public Connection connection;

	// statement对象
	public PreparedStatement statement;

	// 字段名称
	public String[] fieldNames;

	// 缓存大小
	public int bufferLength = 0;

	// 行数
	public long rowCount = 0;

	// 删除数据标示符(true:delete)
	protected boolean dataDeleteFlag = false;

	// 删除数据的主键字段
	protected String[] dataPrimarykeyField;

	/**
	 * 默认构造函数
	 * 
	 * @throws SQLException
	 */
	public DBRecordWriter() throws SQLException {
	}

	/**
	 * 构造方法
	 * 
	 * @param connection
	 *            连接对象
	 * @param statement
	 *            statement对象
	 * @param dbConf
	 *            配置对象
	 * @throws SQLException
	 *             IO异常
	 */
	public DBRecordWriter(Connection connection, PreparedStatement statement, DBConfiguration dbConf)
			throws SQLException {
		this.connection = connection;
		this.statement = statement;
		System.out.println("DBRecordWriter connection old TransactionIsolation=" +
				this.connection.getTransactionIsolation() + " set to TRANSACTION_READ_COMMITTED");
		this.connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		this.fieldNames = dbConf.getOutputFieldNames();
		this.bufferLength = dbConf.getDbOutputBufferLength(0);
		this.dataDeleteFlag = dbConf.getDbOutputDataDeleteFlag();
		this.dataPrimarykeyField = dbConf.getDbOutputDataPrimarykeyField();
		this.connection.setAutoCommit(false);
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		try {
			if (key == null) {
				throw new IOException("write key is null");
			}

			if (this.statement == null) {
				throw new IOException("statement is null");
			}
			if (rowCount < 3)
				System.out.println("rowvalue:" + key);
			if (this.dataDeleteFlag) {// 删除数据
				key.write(this.statement, this.dataPrimarykeyField);
			} else {
				key.write(this.statement, this.fieldNames);
			}
			this.statement.addBatch();
			this.rowCount++;
			if (this.bufferLength > 0 && this.rowCount % this.bufferLength == 0) {
				this.statement.executeBatch();
				this.connection.commit();
			}

			if (this.rowCount % 200000 == 0) {
				LOG.info("insert " + this.rowCount + " rows");
				// MRLog.consoleDebug(LOG, "insert " + this.rowCount + " rows");
			}
		} catch (Exception e) {
			System.err.println("error rowvalue:" + key);
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		try {
			if (this.statement != null && this.connection != null) {
				this.statement.executeBatch();
				this.connection.commit();
				try {
					this.statement.close();
				} catch (SQLException e) {
				}
			}
			LOG.info("Total: insert " + this.rowCount + " rows");
			// MRLog.consoleDebug(LOG, "Total: insert " + this.rowCount +
			// " rows");
		} catch (SQLException e) {
			// try {
			MRLog.errorException(LOG, e);
			// this.connection.rollback();
			// } catch (SQLException ex) {
			// MRLog.errorException(LOG, ex);
			// }
			throw new IOException(e);
		} finally {
			if (this.connection != null) {
				try {
					this.connection.close();
				} catch (SQLException ex) {
				}
			}
			this.statement = null;
			this.connection = null;
		}
	}
}
