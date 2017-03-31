package com.ery.hadoop.mrddx.db.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
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
import com.ery.hadoop.mrddx.MRConfiguration;
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
public class DataDrivenDBInputFormat<T extends DBWritable> extends DBInputFormat<T> implements Configurable,
		IHandleFormat {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(DataDrivenDBInputFormat.class);

	// 条件的替换标识符
	public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

	@SuppressWarnings("unchecked")
	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Class<T> inputClass = (Class<T>) (this.dbConf.getInputClass());
		LOG.debug("Creating db record reader for db product: " + this.dbProductName);

		try {
			Connection conn = this.getConnection();
			// use database product name to determine appropriate record reader.
			if (this.dbProductName.startsWith("MYSQL")) {
				// use MySQL-specific db reader.
				return new MySQLDataDrivenDBRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf,
						this.conditions, this.fieldNames, tableName);
			} else {
				// Generic reader.
				return new DataDrivenDBRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf,
						this.conditions, this.fieldNames, tableName, this.dbProductName);
			}
		} catch (Exception e) {
			MRLog.errorException(LOG, "Get recordReader exception", e);
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

		// public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws
		// IOException {
		int numSplits = context.getConfiguration().getInt(MRConfiguration.MAPRED_MAP_TASKS, 1);

		if (numSplits == 1) { // 无需拆分
			InputSplit inputSplit = new DataDrivenDBInputSplit("1=1", "1=1");
			MRLog.info(LOG, "DBSplitter follow:" + inputSplit.toString());
			List<InputSplit> split = new ArrayList<>();
			split.add(inputSplit);
			return split;
		}

		String boundingQuery = this.getBoundingQuery();
		String orderByColumn = this.dbConf.getInputOrderByColumn();
		ResultSet results = null;
		Statement statement = null;
		Connection connection = null;
		try {
			connection = this.getConnection();
			statement = connection.createStatement();
			results = statement.executeQuery(boundingQuery);
			results.next();

			// 基于结果的第一个字段类型，使用相应的机制 拆分（即：数字分割，文本分割，日期等）
			int sqlDataType = results.getMetaData().getColumnType(1);
			DBSplitter splitter = this.getSplitter(sqlDataType);
			if (null == splitter) {
				String message = "Unknown SQL data type: " + sqlDataType;
				MRLog.error(LOG, message);
				throw new IOException(message);
			}

			MRLog.info(LOG, "DBSplitter type:" + sqlDataType + " Class:" + splitter.getClass().getName());
			List<InputSplit> lstInputSplit = splitter.split(context.getConfiguration(), results, orderByColumn);
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
	 * the DBSplitter implementation to use to divide the table/query into
	 * InputSplits.
	 * 根据OrderBy的字段类型或者'mapred.jdbc.input.bounding.query'查询语句的第一个字段类型创建不同的拆分对象
	 * 
	 * @param sqlDataType
	 *            数据类型
	 * @return DBSplitter 拆分对象
	 */
	protected DBSplitter getSplitter(int sqlDataType) {
		switch (sqlDataType) {
		case Types.NUMERIC:
		case Types.DECIMAL:
			return new BigDecimalSplitter();

		case Types.BIT:
		case Types.BOOLEAN:
			return new BooleanSplitter();

		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.BIGINT:
			return new IntegerSplitter();

		case Types.REAL:
		case Types.FLOAT:
		case Types.DOUBLE:
			return new FloatSplitter();

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return new TextSplitter();

		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return new DateSplitter();

		default:
			// Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB, BLOB,
			// ARRAY,STRUCT, REF,
			// DATALINK, and
			// JAVA_OBJECT.
			return null;
		}
	}

	/**
	 * 获取bounding查询语句(一条数据第一列是最小值，第二列是最大值)
	 * 
	 * @return bounding sql
	 */
	protected String getBoundingQuery() {
		// 自定义语句(若mapreduce.jdbc.input.query查询条件中含有条件，boundingQuery一定要包含条件)
		String userQuery = this.dbConf.getInputBoundingQuery();
		if (null != userQuery && userQuery.trim().length() > 0) {
			MRLog.info(LOG, "Defined boundingQuery:" + userQuery);
			return userQuery;
		}

		// 默认的语句
		StringBuilder query = new StringBuilder();
		String splitColumn = this.dbConf.getInputOrderByColumn();

		query.append("SELECT MIN(").append(splitColumn).append("), ");
		query.append("MAX(").append(splitColumn).append(") FROM ");
		query.append(this.tableName);
		if (null != this.conditions && this.conditions.trim().length() > 0) {
			query.append(" WHERE ( " + this.conditions + " )");
		}

		MRLog.info(LOG, "Default boundingQuery:" + query.toString());
		return query.toString();
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
	public static class DataDrivenDBInputSplit extends DBInputFormat.DBInputSplit {
		private String lowerBoundClause;
		private String upperBoundClause;

		/**
		 * 默认构造方法
		 */
		public DataDrivenDBInputSplit() {
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
		public DataDrivenDBInputSplit(final String lower, final String upper) {
			this.lowerBoundClause = lower;
			this.upperBoundClause = upper;
		}

		@Override
		public long getLength() {
			return 0; // unfortunately, we don't know this.
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			this.lowerBoundClause = Text.readString(input);
			this.upperBoundClause = Text.readString(input);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			Text.writeString(output, this.lowerBoundClause);
			Text.writeString(output, this.upperBoundClause);
		}

		public String getLowerClause() {
			return lowerBoundClause;
		}

		public String getUpperClause() {
			return upperBoundClause;
		}

		@Override
		public String toString() {
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.append("[");
			strBuilder.append(lowerBoundClause);
			strBuilder.append(",");
			strBuilder.append(upperBoundClause);
			strBuilder.append("]");
			return strBuilder.toString();
		}
	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setInputFormatClass(DataDrivenDBInputFormat.class);
	}

	@Override
	protected void validateInputParameter(DBConfiguration dbconf) throws Exception {
		String query = dbconf.getInputQuery();
		String tableName = dbconf.getInputTableName();
		String fieldNames[] = dbconf.getInputFieldNames();
		String conditions = dbconf.getInputConditions();
		String boundingQuery = dbconf.getInputBoundingQuery();
		String orderBy = dbconf.getInputOrderByColumn();

		boolean isNullpInputQuery = (null == query || query.trim().length() <= 0);
		boolean isNullpInputTableName = (null == tableName || tableName.trim().length() <= 0);
		boolean isNullpInputFieldNames = (null == fieldNames || fieldNames.length <= 0);
		boolean isNullpInputConditions = (null == conditions || conditions.trim().length() <= 0);
		boolean isNullpInputBoundingQuery = (null == boundingQuery || boundingQuery.trim().length() <= 0);
		boolean isNullpInputOrderByColumn = (null == orderBy || orderBy.trim().length() <= 0);

		// 通过拆分类型验证
		String meg = "";
		// A.参数设置(必填：inputQuery, inputBoundingQuery, inputOrderByColumn,
		// 可选：inputTableName,inputFieldNames)
		if (!isNullpInputQuery && !isNullpInputBoundingQuery && !isNullpInputOrderByColumn) {
			if (!(query.indexOf("$CONDITIONS") > 0)) {
				meg = "源查询语句<InputQuery>必须包含'$CONDITIONS'内容,否则无法实现数据驱动拆分.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			if (isNullpInputTableName) {
				if (isNullpInputFieldNames) {
					meg = "源字段<inputTableName>未设置的情况下，表名<inputFieldNames>不能为空.";
					MRLog.error(LOG, meg);
					throw new Exception(meg);
				}
				MRLog.warn(LOG, "源字段<inputTableName>未设置,默认为空.");
			}

			if (isNullpInputFieldNames) {
				if (isNullpInputTableName) {
					meg = "源字段<inputFieldNames>未设置的情况下，表名<inputTableName>不能为空.";
					MRLog.error(LOG, meg);
					throw new Exception(meg);
				}
				MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为查询语句<inputQuery>的字段.");
			}
		}

		// b.参数设置(必填：inputQuery, inputTableName, inputOrderByColumn,
		// 可选：inputBoundingQuery,inputFieldNames,inputConditions)
		if (!isNullpInputQuery && !isNullpInputOrderByColumn && !isNullpInputTableName) {
			if (isNullpInputFieldNames) {
				MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为查询语句<inputQuery>的字段.");
			}

			if (isNullpInputBoundingQuery) {
				MRLog.warn(LOG, "绑定查询<inputBoundingQuery>未设置,默认通过<inputOrderByColumn>拼接.");
			}

			if (isNullpInputConditions) {
				MRLog.warn(LOG,
						"查询条件<inputConditions>未设置, 默认为空.注：若<inputQuery>有除了$CONDITIONS之外的查询语句，那<inputConditions>必须和其一致");
			}
		}

		// c.参数设置(必填：inputBoundingQuery, inputTableName, inputOrderByColumn,
		// 可选:inputQuery,inputFieldNames,inputConditions)
		if (!isNullpInputBoundingQuery && !isNullpInputTableName && !isNullpInputOrderByColumn) {
			if (isNullpInputQuery) {
				MRLog.warn(LOG, "源查询语句<inputQuery>未设置,为默认值.");
			}

			if (isNullpInputFieldNames) {
				MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为查询语句<inputQuery>的字段.");
			}

			if (isNullpInputConditions) {
				MRLog.warn(LOG, "查询条件<inputConditions>未设置, 默认为空.注：若<inputBoundingQuery>含有查询语句，那<inputConditions>必须和其一致");
			}
		}

		// d.参数设置(必填： inputTableName, inputOrderByColumn,
		// 可选:inputQuery,inputBoundingQuery,inputFieldNames,inputConditions)
		if (!isNullpInputTableName && !isNullpInputOrderByColumn) {
			if (isNullpInputQuery) {
				MRLog.warn(LOG, "源查询语句<inputQuery>未设置,为默认值.");
			}

			if (isNullpInputBoundingQuery) {
				MRLog.warn(LOG, "绑定查询<inputBoundingQuery>未设置,默认通过<inputOrderByColumn>拼接.");
			}

			if (isNullpInputFieldNames) {
				MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为表的所有字段.");
			}

			if (isNullpInputConditions) {
				MRLog.warn(LOG, "查询条件<inputConditions>未设置, 默认为空.");
			}
		}
	}
}
