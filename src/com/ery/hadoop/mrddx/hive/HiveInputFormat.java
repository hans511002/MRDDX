package com.ery.hadoop.mrddx.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
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
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * Hive输入格式
 * 



 * @createDate 2013-1-18
 * @version v1.0
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HiveInputFormat<T extends DBWritable> extends InputFormat<LongWritable, T> implements Configurable,
		IHandleFormat {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(HiveInputFormat.class);

	// 连接对象
	private Connection connection;

	// 查询sql
	private String querySql;

	// 字段名称列表
	private String[] fieldNames;

	// hive的配置对象
	private HiveConfiguration hiveConf;

	@Override
	public void setConf(Configuration conf) {
		this.hiveConf = new HiveConfiguration(conf);
		this.querySql = this.hiveConf.getInputHiveQuery();
		this.fieldNames = this.hiveConf.getInputFieldNames();

		long logId = conf.getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		MRLog.info(LOG, logId, "Connect hive begin!");
		try {
			// 获取连接
			this.connection = this.getConnection();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		MRLog.info(LOG, logId, "Connect hive end!!");
	}

	@SuppressWarnings("unchecked")
	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		// public RecordReader<LongWritable, T> getRecordReader(InputSplit
		// split, JobConf job, Reporter reporter)
		// throws IOException {
		Class<T> inputClass = (Class<T>) (this.hiveConf.getInputClass());
		return new HiveRecordReader<T>((HiveInputSplit) split, inputClass, context.getConfiguration(),
				this.getConnection(), this.querySql, this.fieldNames);
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		// public InputSplit[] getSplits(JobConf job, int numSplits) throws
		// IOException {
		List<InputSplit> split = new ArrayList<InputSplit>();
		split.add(new HiveInputSplit());
		MRLog.info(LOG, "Finished hive split!");
		return split;
	}

	/**
	 * 获取连接
	 * 
	 * @return connection 连接
	 * @throws IOException IO异常
	 */
	public Connection getConnection() throws IOException {
		if (null != this.connection) {
			return this.connection;
		}

		Connection conn = null;
		try {
			conn = this.hiveConf.getInputConnection();
		} catch (Exception e) {
			MRLog.info(LOG, "create connection error!");
			throw new IOException(e);
		}

		return conn;
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
		} catch (SQLException sqlE) {
			MRLog.info(LOG, "close connection error!");
		}
	}

	/**
	 * Initializes the map-part of the job with the appropriate input settings.
	 * 
	 * @param job The map-reduce job
	 * @param inputClass the class object implementing DBWritable, which is the
	 *            Java object holding tuple fields.
	 * @param inputQuery the input query to select fields. Example :
	 *            "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
	 * @param inputCountQuery the input query that returns the number of records
	 *            in the table. Example : "SELECT COUNT(f1) FROM Mytable"
	 */
	public static void setInput(Job job, Class<? extends DBWritable> inputClass, String inputQuery) {
		job.setInputFormatClass(HiveInputFormat.class);
		HiveConfiguration dbConf = new HiveConfiguration(job.getConfiguration());
		dbConf.setInputClass(inputClass);
		dbConf.setInputHiveQuery(inputQuery);
	}

	@Override
	public Configuration getConf() {
		return hiveConf.getConf();
	}

	public HiveConfiguration getDBConf() {
		return hiveConf;
	}

	/**
	 * Hive的拆分类
	 * 



	 * @createDate 2013-1-18
	 * @version v1.0
	 */
	public static class HiveInputSplit extends InputSplit implements Writable {
		private long start;
		private long end;

		public HiveInputSplit() {
		}

		@Override
		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		public long getStart() {
			return 0;
		}

		public long getEnd() {
			return 0;
		}

		@Override
		public long getLength() throws IOException {
			return 0;
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
	}

	@Override
	public void handle(Job conf) throws Exception {
		HiveConfiguration hconf = new HiveConfiguration(conf.getConfiguration());

		String url = hconf.getInPutHiveConfigUrl();
		if (null == url || url.trim().length() <= 0) {
			String meg = "Hive数据库地址<" + HiveConfiguration.INPUT_HIVE_CONFIG_URL + ">不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		String user = hconf.getInPutHiveConfigUser();
		if (null == user || user.trim().length() <= 0) {
			MRLog.warn(LOG, "hive数据库用户名<" + HiveConfiguration.INPUT_HIVE_CONFIG_USER + ">为空.");
		}

		String pwd = hconf.getInPutHiveConfigPassword();
		if (null == pwd || pwd.trim().length() <= 0) {
			MRLog.warn(LOG, "hive数据库密码<" + HiveConfiguration.INPUT_HIVE_CONFIG_PASSWORD + ">为空.");
		}

		String para = hconf.getInputHiveQuery();
		if (null == para || para.trim().length() <= 0) {
			String meg = "Hive查询sql语句<" + HiveConfiguration.INPUT_HIVE_QUERY + ">不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		hconf.setInputClass(DBRecord.class);
		conf.setInputFormatClass(HiveInputFormat.class);
		conf.setMapOutputKeyClass(DBRecord.class);
		conf.setMapOutputValueClass(DBRecord.class);
		if (hconf.getInputIsCombiner()) {
			conf.setCombinerClass(DBGroupReducer.class);
			conf.setCombinerKeyGroupingComparatorClass(DBRecord.class);
		}
		conf.setMapperClass(DBMapper.class);
	}
}
