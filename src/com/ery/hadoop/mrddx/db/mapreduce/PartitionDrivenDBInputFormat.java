package com.ery.hadoop.mrddx.db.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBInputFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * 

 * 

 * @Comments 从表中读取数据的输入格式对象，类似于DBInputFormat，但不是用LIMIT和OFFSET来拆分，是数据类型驱动拆分

 * @version v1.0
 * @create Data 2013-1-7
 * 
 * @param <T>
 *            DBWritable
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PartitionDrivenDBInputFormat<T extends DBWritable> extends DBInputFormat<T> implements Configurable,
		IHandleFormat {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(PartitionDrivenDBInputFormat.class);

	// 条件的替换标识符
	public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

	@SuppressWarnings("unchecked")
	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// public RecordReader<LongWritable, T> getRecordReader(InputSplit
		// split, JobConf job, Reporter reporter)
		// throws IOException {
		Class<T> inputClass = (Class<T>) (this.dbConf.getInputClass());
		LOG.debug("Creating db record reader for db product: " + this.dbProductName);
		try {
			Connection conn = this.getConnection();
			// use database product name to determine appropriate record reader.
			// Generic reader.
			return new PartitionDrivenDBRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf,
					this.conditions, this.fieldNames, tableName);
		} catch (Exception e) {
			MRLog.errorException(LOG, "Get recordReader exception", e);
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		ResultSet results = null;
		Statement statement = null;
		Connection connection = null;
		try {
			String boundingQuery = this.getPartitionQuery();
			if (boundingQuery == null) {
				throw new Exception("config error: Patrtition SQL is null");
			}
			connection = this.getConnection();
			statement = connection.createStatement();
			results = statement.executeQuery(boundingQuery);
			List<InputSplit> lstInputSplit = new ArrayList<InputSplit>();
			while (results.next()) {
				lstInputSplit.add(new PartitionDrivenDBInputSplit(results.getString(1)));
			}
			MRLog.info(
					LOG,
					"DBSplitter size:" + lstInputSplit.size() + " content:"
							+ StringUtil.parseListToString(lstInputSplit, " "));
			return lstInputSplit;
		} catch (Exception e) {
			String message = "Get splits exception: ";
			MRLog.errorException(LOG, message, e);
			throw new IOException(StringUtil.stringifyException(e));
		} finally {
			try {
				if (null != results) {
					results.close();
				}
			} catch (SQLException e) {
				MRLog.warnException(LOG, "SQLException closing resultset: ", e);
			}

			try {
				if (null != statement) {
					statement.close();
				}
			} catch (SQLException e) {
				MRLog.warnException(LOG, "SQLException closing statement: ", e);
			}

			try {
				connection.commit();
				closeConnection();
			} catch (SQLException e) {
				MRLog.warnException(LOG, "SQLException committing split transaction: ", e);
			}
		}
	}

	/**
	 * 获取查询分区语句
	 * 
	 * @return bounding sql
	 */
	protected String getPartitionQuery() {
		// 自定义语句
		String userQuery = this.dbConf.getInputPartitionQuery();
		if (null != userQuery && userQuery.trim().length() > 0) {
			MRLog.info(LOG, "Defined Partition query:" + userQuery);
			return userQuery;
		}
		return null;
	}

	/**
	 * 


	 * 

	 * @Comments A InputSplit that spans a set of rows

	 * @version v1.0
	 * @create Data 2013-1-8
	 * 
	 */
	@InterfaceStability.Evolving
	public static class PartitionDrivenDBInputSplit extends DBInputFormat.DBInputSplit {
		public String partitionName;

		/**
		 * 默认构造方法
		 */
		public PartitionDrivenDBInputSplit() {
		}

		/**
		 * Convenience Constructor
		 * 
		 * @param lower
		 *            the string to be put in the WHERE clause to guard on the
		 *            'lower' end
		 * @param upper
		 *            the string to be put in the WHERE clause to guard on the
		 *            'upper' end
		 */
		public PartitionDrivenDBInputSplit(final String partitionName) {
			this.partitionName = partitionName;
		}

		@Override
		public long getLength() {
			return 0; // unfortunately, we don't know this.
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			this.partitionName = Text.readString(input);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			Text.writeString(output, this.partitionName);
		}

		@Override
		public String toString() {
			return partitionName;
		}
	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setInputFormatClass(PartitionDrivenDBInputFormat.class);
	}

	@Override
	protected void validateInputParameter(DBConfiguration dbconf) throws Exception {
		String query = dbconf.getInputQuery();
		String partitionQuery = dbconf.getInputPartitionQuery();
		String tablename = dbconf.getInputTableName();

		boolean isNullpInputQuery = (null == query || query.trim().length() <= 0);
		boolean isNullpInputPartitionQuery = (null == partitionQuery || partitionQuery.trim().length() <= 0);
		boolean isNullTablename = (null == tablename || tablename.trim().length() <= 0);

		// 通过拆分类型验证
		String meg = "";
		// A.参数设置(必填：query, partitionQuery)
		if (isNullpInputPartitionQuery) {
			meg = "查询分区语句<partitionQuery>不能为空,否则无法实现分区拆分.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (isNullTablename) {
			meg = "查询分区语句<inputTableName>不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (!isNullpInputQuery) {
			if (!(query.indexOf("{SRC_TABLE}") > 0)) {
				meg = "源查询语句<inputQuery>必须包含'{SRC_TABLE}'内容,否则无法实现分区拆分.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}
	}
}
