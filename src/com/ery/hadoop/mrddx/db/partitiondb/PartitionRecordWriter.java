package com.ery.hadoop.mrddx.db.partitiondb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBRecordWriter;
import com.ery.hadoop.mrddx.db.partitiondb.route.PartitionFunction;
import com.ery.base.support.utils.Utils;

public class PartitionRecordWriter<K extends DBRecord, V> extends RecordWriter<K, V> {
	TaskAttemptContext context;
	Configuration conf;
	PartitionFunction partition;
	String[] partitionUrls;
	String insertSql = null;
	String partitionFiledName;
	int beachSize = 3000;
	long allRowCount = 0;
	long[] parRowCount = null;
	public Exception ex = null;
	SubmitThread[] runThread = null;
	boolean isRunning = true;

	public PartitionRecordWriter(TaskAttemptContext context, PartitionFunction partition, String partitionFiledName,
			String[] partitionIps, String insertSql) throws IOException {
		this.context = context;
		init(context.getConfiguration(), partition, partitionFiledName, partitionIps, insertSql);
	}

	PartitionRecordWriter(Configuration conf, PartitionFunction partition, String partitionFiledName,
			String[] partitionIps, String insertSql) throws IOException {
		this.context = null;
		init(conf, partition, partitionFiledName, partitionIps, insertSql);
	}

	public void init(Configuration conf, PartitionFunction partition, String partitionFiledName, String[] partitionIps,
			String insertSql) throws IOException {
		this.conf = conf;
		this.partition = partition;
		this.partitionUrls = partitionIps;
		this.partitionFiledName = partitionFiledName;
		this.insertSql = insertSql;
		parRowCount = new long[partitionIps.length];
		runThread = new SubmitThread[partitionIps.length];
		this.beachSize = conf.getInt(DBConfiguration.DB_OUTPUT_BUFFER_LENGTH, 3000);
		for (int i = 0; i < partitionIps.length; i++) {
			runThread[i] = new SubmitThread(this, partitionIps[i]);
			runThread[i].setDaemon(true);
			runThread[i].setName("SubmitThread");
			if (this.context != null)
				runThread[i].start();
		}
	}

	// 停止子线程，关闭连接等
	public void close(TaskAttemptContext context) throws IOException {
		System.out.println("开始关闭处理线程");
		while (true) {
			boolean isAllEnd = true;
			for (SubmitThread thread : runThread) {
				synchronized (thread.metux) {
					thread.metux.notifyAll();
				}
				if (!thread.end)
					isAllEnd = false;
				if (ex != null) {
					throw new IOException(ex);
				}
			}
			if (isAllEnd)
				break;
			if (context != null) {
				context.setStatus("等待子处理线程结束  allRowCount:" + this.allRowCount);
			}
			System.out.println("等待子处理线程结束");
			Utils.sleep(500);
			isRunning = false;
		}
		if (context != null) {
			context.getCounter("split output", "allRowCount").increment(allRowCount);
			for (int i = 0; i < parRowCount.length; i++) {
				context.getCounter("split output", "partition[" + i + "]").increment(parRowCount[i]);
			}
		}
	}

	public Connection getOutputConnection(String hostUrl) throws ClassNotFoundException, SQLException {
		System.out.println("连接数据库:" + hostUrl);
		DBConfiguration dbConf = new DBConfiguration(this.conf);
		String driver = dbConf.getDriverName(hostUrl, false);
		String user = this.conf.get(DBConfiguration.OUTPUT_USERNAME_PROPERTY, "");
		String pwd = this.conf.get(DBConfiguration.OUTPUT_PASSWORD_PROPERTY, "");
		Class.forName(driver);
		if (null == user) {
			return DriverManager.getConnection(hostUrl);
		} else {
			return DriverManager.getConnection(hostUrl, user, pwd);
		}
	}

	public static String constructQuery(String table, String[] fieldNames) {
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

	// 提交给子线程去提交数据，其它服务端的连接就不用等待
	public static class SubmitThread extends HasThread {
		public java.util.LinkedList<Map<String, Object>> records = new LinkedList<Map<String, Object>>();
		DBRecordWriter<DBRecord, ?> dbWriter;
		final PartitionRecordWriter write;
		final Object metux = new Object();
		boolean end = true;
		String hostUrl;
		// 连接对象
		public Connection connection;
		// statement对象
		public PreparedStatement statement;

		public SubmitThread(PartitionRecordWriter<?, ?> write, String hostUrl) throws IOException {
			try {
				this.write = write;
				this.hostUrl = hostUrl;
				connection = write.getOutputConnection(hostUrl);
				connection.setAutoCommit(false);
				connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				if (write.insertSql != null) {
					statement = connection.prepareStatement(write.insertSql);
				}
				this.dbWriter = new DBRecordWriter<>(connection, statement, new DBConfiguration(write.conf));
				this.dbWriter.bufferLength = write.beachSize;
			} catch (SQLException e) {
				throw new IOException(e);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		public void close() throws IOException {
			try {
				System.out.println("关闭子处理线程:" + hostUrl);
				if (dbWriter != null)
					dbWriter.close(write.context);
				statement = null;
				connection = null;
				dbWriter = null;
			} catch (IOException e) {
				throw e;
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void run() {
			end = false;
			while (true) {
				try {
					synchronized (metux) {
						metux.wait(0);
					}
				} catch (InterruptedException e) {
				}
				try {
					Map<String, Object> map = null;
					synchronized (records) {
						if (!records.isEmpty())
							map = records.removeFirst();
						records.notifyAll();
					}
					while (map != null) {
						dbWriter.write(new DBRecord(map), null);
						map = null;
						synchronized (records) {
							if (!records.isEmpty())
								map = records.removeFirst();
							records.notifyAll();
						}
					}
					if (!write.isRunning || write.ex != null) {
						close();
						end = true;
						break;
					}
					// Utils.sleep(500);
				} catch (Exception e) {
					synchronized (write) {
						write.ex = e;
						end = true;
						break;
					}
				} finally {
				}
			}
			end = true;
		}

		public void add(Map<String, Object> record) throws InterruptedException {
			synchronized (records) {
				records.add(record);
				if (records.size() > write.beachSize / 6) {
					synchronized (metux) {
						metux.notifyAll();
					}
				}
			}
		}
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		if (key == null)
			return;
		if (ex != null) {
			throw new IOException(ex);
		}
		Map<String, Object> record = key.getRow();
		Object imsi = record.get(partitionFiledName);
		if (imsi == null) {
			System.err.println("partition key is null row:" + record);
			this.context.getCounter("split output", "null partitionKey").increment(1);
			return;
		}
		int partitionIndex = partition.calculate(imsi);
		runThread[partitionIndex].add(record);
		parRowCount[partitionIndex]++;

		while (runThread[partitionIndex].records.size() > beachSize * 3) {
			for (int i = 0; i < runThread.length; i++) {
				synchronized (runThread[i].metux) {
					runThread[i].metux.notifyAll();
				}
			}
			synchronized (runThread[partitionIndex].records) {
				runThread[partitionIndex].records.wait(0);
			}
			// Utils.sleep(500);
		}
		allRowCount++;
	}

}
