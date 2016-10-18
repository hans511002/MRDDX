/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.ery.hadoop.mrddx.db.rdbnorm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBInputFormat;
import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.vertica.Relation;
import com.ery.base.support.utils.Convert;

public class RDBInputFormat<T extends DBWritable> extends DBInputFormat<T> implements Configurable, IHandleFormat {
	private static final Log LOG = LogFactory.getLog(RDBInputFormat.class);

	public static final String INPUT_QUERY_SQL = "mapred.rdb.input.querysql";
	public static final String INPUT_QUERY_SQL_PARAMS = "mapred.rdb.input.query.params";
	public static final String INPUT_QUERY_SQL_MACRO = "mapred.rdb.input.query.macro.names";

	private String inputQuery = null;
	private String params = null;

	public RDBInputFormat() {
	}

	public RDBInputFormat(String query, String params) {
		inputQuery = query;
		this.params = params;
	}

	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		Class<T> inputClass = (Class<T>) (this.dbConf.getInputClass());
		try {
			Connection conn = this.getConnection();
			MRLog.info(LOG, logId, "RecordReader:RDBRecordReader");
			return new RDBRecordReader<T>(split, inputClass, this.conf, conn, this.dbConf, this.conditions,
					this.fieldNames, this.tableName);
		} catch (Exception e) {
			MRLog.errorException(LOG, "Get recordReader exception", e);
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Set the input query for a job
	 * 
	 * @param job
	 * @param inputQuery
	 *            query to run against Vertica
	 */
	public static void setInput(Job job, String inputQuery) {
		job.setInputFormatClass(RDBInputFormat.class);
		inputQuery = inputQuery.trim();
		if (inputQuery.endsWith(";")) {
			inputQuery = inputQuery.substring(0, inputQuery.length() - 1);
		}
		job.getConfiguration().set(INPUT_QUERY_SQL, inputQuery);
	}

	/**
	 * Set a parameterized input query for a job and the query that returns the
	 * parameters.
	 * 
	 * @param job
	 * @param inputQuery
	 *            SQL query that has parameters specified by question marks
	 *            ("?")
	 * @param segmentParamsQuery
	 *            SQL query that returns parameters for the input query
	 */

	public static void setInput(Job job, String inputQuery, String segmentParamsQuery) {
		job.setInputFormatClass(RDBInputFormat.class);
		inputQuery = inputQuery.trim();
		if (inputQuery.endsWith(";")) {
			inputQuery = inputQuery.substring(0, inputQuery.length() - 1);
		}
		job.getConfiguration().set(INPUT_QUERY_SQL, inputQuery);
		if (segmentParamsQuery != null)
			job.getConfiguration().set(INPUT_QUERY_SQL_PARAMS, segmentParamsQuery);
	}

	/**
	 * Set the input query and any number of comma delimited literal list of
	 * parameters
	 * 
	 * @param job
	 * @param inputQuery
	 *            SQL query that has parameters specified by question marks
	 *            ("?")
	 * @param segmentParams
	 *            any numer of comma delimited strings with literal parameters
	 *            to substitute in the input query
	 */

	@SuppressWarnings("serial")
	public static void setInput(Job job, String inputQuery, String... segmentParams) throws IOException {
		// transform each param set into array
		DateFormat datefmt = DateFormat.getDateInstance();
		Collection<List<Object>> params = new HashSet<List<Object>>() {
		};
		for (String strParams : segmentParams) {
			List<Object> param = new ArrayList<Object>();

			for (String strParam : strParams.split(",")) {
				strParam = strParam.trim();
				if (strParam.charAt(0) == '\'' && strParam.charAt(strParam.length() - 1) == '\'')
					param.add(strParam.substring(1, strParam.length() - 1));
				else {
					try {
						param.add(datefmt.parse(strParam));
					} catch (ParseException e1) {
						try {
							param.add(Integer.parseInt(strParam));
						} catch (NumberFormatException e2) {
							throw new IOException("Error parsing argument " + strParam);
						}
					}
				}
			}

			params.add(param);
		}

		setInput(job, inputQuery, params, null);
	}

	/**
	 * Set the input query and a collection of parameter lists
	 * 
	 * @param job
	 * @param inpuQuery
	 *            SQL query that has parameters specified by question marks
	 *            ("?")
	 * @param segmentParams
	 *            collection of ordered lists to subtitute into the input query
	 * @throws IOException
	 */

	public static void setInput(Job job, String inputQuery, Collection<List<Object>> segmentParams,
			List<Integer> macroIndexs) throws IOException {
		job.setInputFormatClass(RDBInputFormat.class);
		inputQuery = inputQuery.trim();
		if (inputQuery.endsWith(";")) {
			inputQuery = inputQuery.substring(0, inputQuery.length() - 1);
		}
		job.getConfiguration().set(INPUT_QUERY_SQL, inputQuery);
		if (segmentParams != null && segmentParams.size() > 0) {
			String[] values = new String[segmentParams.size()];
			int i = 0;
			for (List<Object> params : segmentParams) {
				DataOutputBuffer out = new DataOutputBuffer();
				out.writeInt(params.size());
				for (Object obj : params) {
					int type = Relation.getType(obj);
					out.writeInt(type);
					Relation.write(obj, type, out);
				}
				values[i++] = StringUtils.byteToHexString(out.getData());
			}
			job.getConfiguration().setStrings(INPUT_QUERY_SQL_PARAMS, values);
		}
		if (macroIndexs != null && macroIndexs.size() > 0) {
			String macStr = "" + macroIndexs.get(0);
			for (Integer mac : macroIndexs) {
				macStr += "," + mac;
			}
			job.getConfiguration().set(INPUT_QUERY_SQL_MACRO, macStr);
		}
	}

	public Collection<List<Object>> getInputParameters() throws IOException {
		Collection<List<Object>> values = null;
		String[] query_params = conf.getStrings(INPUT_QUERY_SQL_PARAMS);
		if (query_params != null) {
			values = new ArrayList<List<Object>>();
			for (String str_params : query_params) {
				DataInputBuffer in = new DataInputBuffer();
				in.reset(StringUtils.hexStringToByte(str_params), str_params.length());
				int sz = in.readInt();
				ArrayList<Object> params = new ArrayList<Object>();
				for (int count = 0; count < sz; count++) {
					int type = in.readInt();
					params.add(Relation.readField(type, in));
				}
				values.add(params);
			}
		}
		return values;
	}

	/** {@inheritDoc} */
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		long numSplits = conf.getInt(MRConfiguration.MAPRED_MAP_TASKS, 1);
		LOG.debug("creating splits up to " + numSplits);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		int i = 0;

		inputQuery = conf.get(INPUT_QUERY_SQL, null);
		if (inputQuery == null)
			throw new IOException("rdb input requires query defined by " + INPUT_QUERY_SQL);
		params = conf.get(INPUT_QUERY_SQL_PARAMS, null);
		String macroParams[] = conf.getStrings(INPUT_QUERY_SQL_PARAMS);
		int macroParamIndex[] = null;
		if (macroParams != null && macroParams.length > 0) {
			boolean isInt = true;
			int macroIndex[] = new int[macroParams.length];
			int i1 = 0;
			for (String mp : macroParams) {
				int index = Convert.toInt(mp, -1);
				if (index == -1) {
					isInt = false;
					break;
				}
				macroIndex[i1++] = index;
			}
			if (isInt) {
				macroParamIndex = macroIndex;
			}
		}

		boolean isSelectSQL = true;
		Collection<List<Object>> paramCollection = null;
		if (params != null && params.trim().substring(0, 10).toUpperCase().startsWith("SELECT")) {
			paramCollection = getInputParameters();
			isSelectSQL = false;
		}
		if (isSelectSQL) {
			LOG.debug("creating splits using paramsQuery :" + params);
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = getConnection();
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(params);
				ResultSetMetaData rsmd = rs.getMetaData();
				if (macroParams != null && macroParamIndex == null) {
					macroParamIndex = new int[macroParams.length];
					Map<String, Integer> namerel = new HashMap<String, Integer>();
					for (int c = 1; c <= rsmd.getColumnCount(); c++) {
						namerel.put(rsmd.getColumnName(c), c - 1);
					}
					for (int j = 0; j < macroParams.length; j++) {
						if (namerel.containsKey(macroParams[j]))
							macroParamIndex[j] = namerel.get(macroParams[j]);
						else
							throw new IOException("宏变量名称不在查询SQL输出字段中");
					}
				} else if (macroParamIndex != null) {
					for (int j = 0; j < macroParamIndex.length; j++) {
						if (macroParamIndex[j] >= rsmd.getColumnCount()) {
							throw new IOException("宏变量索引大于查询SQL的输出字段数");
						}
					}
				}
				while (rs.next()) {
					List<Object> segmentParams = new ArrayList<Object>();
					for (int j = 1; j <= rsmd.getColumnCount(); j++) {
						segmentParams.add(rs.getObject(j));
					}
					if (macroParamIndex != null && macroParamIndex.length > 0) {
						String tmpSql = inputQuery.replaceAll("\\{" + rsmd.getColumnName(macroParamIndex[0] + 1) +
								"\\}", segmentParams.get(macroParamIndex[0]).toString());
						List<Object> segment = new ArrayList<Object>();
						for (int j = 1; j < macroParamIndex.length; j++) {
							tmpSql = tmpSql.replaceAll("\\{" + rsmd.getColumnName(macroParamIndex[j] + 1) + "\\}",
									segmentParams.get(macroParamIndex[j]).toString());
							segment.add(segmentParams.get(macroParamIndex[j]));
						}
						segmentParams.removeAll(segment);
						splits.add(new RDBInputSplit(tmpSql, segmentParams));
					} else {
						splits.add(new RDBInputSplit(inputQuery, segmentParams));
					}
				}
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					if (stmt != null)
						stmt.close();
					conn.close();
				} catch (SQLException e) {
					throw new IOException(e);
				}
			}
		} else if (paramCollection != null) {
			LOG.debug("creating splits using " + paramCollection.size() + " params");
			for (List<Object> segmentParams : paramCollection) {
				splits.add(new RDBInputSplit(inputQuery, segmentParams));
			}
		} else if (numSplits > 1) {
			LOG.debug("creating splits using limit and offset");
			Connection conn = null;
			Statement stmt = null;
			long count = 0;
			long start = 0;
			long end = 0;
			String countQuery = "SELECT COUNT(*) FROM (\n" + inputQuery + "\n) count";
			try {
				conn = getConnection();
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(countQuery);
				rs.next();
				count = rs.getLong(1);
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException e) {
					throw new IOException(e);
				}
			}

			long splitSize = count / numSplits;
			end = splitSize;

			LOG.debug("creating " + numSplits + " splits for " + count + " records");

			for (i = 1; i < numSplits; i++) {
				splits.add(new RDBInputSplit(inputQuery, start, end));
				LOG.debug("Split(" + i + "), start:" + start + ", end:" + end);
				start += splitSize;
				end += splitSize;
				count -= splitSize;
			}
			if (count > 0) {
				splits.add(new RDBInputSplit(inputQuery, start, count));
			}
		} else {
			splits.add(new RDBInputSplit(inputQuery, 0, 0));
		}

		// LOG.debug("creating splits using " + params + " params");
		// for (String strParam : params.split(",")) {
		// strParam = strParam.trim();
		// if (strParam.charAt(0) == '\'' && strParam.charAt(strParam.length() -
		// 1) == '\'')
		// strParam = strParam.substring(1, strParam.length() - 1);
		// List<Object> segmentParams = new ArrayList<Object>();
		// segmentParams.add(strParam);
		// splits.add(new RDBInputSplit(inputQuery, segmentParams));
		// }

		LOG.debug("returning " + splits.size() + " final splits");
		return splits;
	}

	protected void validateInputParameter(DBConfiguration dbconf) throws Exception {
	}

	@Override
	public void handle(Job job) throws Exception {
		Configuration conf = job.getConfiguration();
		job.setInputFormatClass(RDBInputFormat.class);
		super.handle(job);
		job.setInputFormatClass(RDBInputFormat.class);

		// conf.setMapperClass();
		// config.setInputQuery(inpuQuery);
		// config.setInputParams(segmentParams);

		// conf.setMapperClass(DBFileMapper.class);
		// conf.setMapperClass(DBHFileMapper.class);
		// conf.setInputFormatClass(HBaseHFileInputFormat.class);
		//
		// conf.setMapOutputKeyClass(ImmutableBytesWritable.class);
		// conf.setMapOutputValueClass(KeyValue.class);

	}
}
