/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ery.hadoop.mrddx.hive;

import java.io.IOException;
import java.sql.Connection;
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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBPartitionReducer;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.mapreduce.FileWritable;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**
 * Hive的输出格式
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-18
 * @version v1.0
 * @param <K>
 * @param <V>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HiveOutputFormat<K extends FileWritable, V> extends FileOutputFormat<K, NullWritable> implements
		IHandleFormat {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(HiveOutputFormat.class);

	@Override
	public RecordWriter<K, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new HiveRecordWriter<K, NullWritable>(context, this);
		//
		// // public RecordWriter<K, NullWritable> getRecordWriter(FileSystem
		// // ignored, JobConf job, String name,
		// // Progressable progress) throws IOException {
		// HiveConfiguration hiveConf = new
		// HiveConfiguration(context.getConfiguration());
		//
		// String fieldSeparator = hiveConf.getOutputHiveFileFieldSplitChars();
		// String rowSeparator = hiveConf.getOutputHiveFileRowsSplitChars();
		// String[] fieldNames = hiveConf.getOutputFieldNames();
		// boolean isCompressed = hiveConf.getOutputHiveCompress();
		//
		// // 无需压缩
		// if (!isCompressed) {
		// // Path file = FileOutputFormat.getTaskOutputPath(context);
		// String name = context.getConfiguration().get(name, "outData");
		// String extension = ".data";
		// Path file = null;
		// file = FileOutputFormat.getPathForWorkFile((TaskInputOutputContext)
		// context, name, extension);
		// file = this.getDefaultWorkFile(context, extension);
		//
		// FileSystem fs = FileSystem.get(context.getConfiguration());//
		// file.getFileSystem(job);
		// FSDataOutputStream fileOut = fs.create(file, context);
		//
		// }
		//
		// // 需要压缩
		// // 获取压缩标识
		// String compresseCodec = hiveConf.getOutputHiveCompressCodec();
		// CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec,
		// context.getConfiguration());
		// String name =
		// context.getConfiguration().get(getOutputHiveOrderFileNamePrefix,
		// "outData");
		// String extension = codec.getDefaultExtension();
		// Path file = null;
		// file = FileOutputFormat.getPathForWorkFile((TaskInputOutputContext)
		// context, name, extension);
		// file = this.getDefaultWorkFile(context, extension);
		//
		// // build the filename including the extension
		// Path file = FileOutputFormat.getTaskOutputPath(job, name +
		// codec.getDefaultExtension());
		// FileSystem fs = file.getFileSystem(job);
		// FSDataOutputStream fileOut = fs.create(file, progress);
		// DataOutputStream dos = new
		// DataOutputStream(codec.createOutputStream(fileOut));
		// return new HiveRecordWriter<K, NullWritable>(job, dos,
		// fieldSeparator, rowSeparator, fieldNames);
	}

	/**
	 * 初始化配置信息
	 * 
	 * @param job The job
	 * @param tableName The table to insert data into
	 * @param fieldNames The field names in the table.
	 * @param ddlHQL require execute HQL before mapreduce running
	 */
	public static void setOutputParameter(Configuration job, boolean compress, String compressCodec,
			String fieldSplitChars, String rowsSplitChars, String ddlHQL) {
		HiveConfiguration hiveConf = new HiveConfiguration(job);
		hiveConf.setOutputHiveCompress(compress);
		hiveConf.setOutputHiveCompressCodec(compressCodec);
		hiveConf.setOutputHiveFileFieldSplitChars(fieldSplitChars);
		hiveConf.setOutputHiveFileRowsSplitChars(rowsSplitChars);
		hiveConf.setOutputHiveExecuteDDLHQL(ddlHQL);
		try {
			executeDDLHQL(hiveConf);
			MRLog.info(LOG, "execute ddl hive sql success!");
		} catch (SQLException e) {
			MRLog.error(LOG, "execute ddl hive sql error!");
			e.printStackTrace();
		}
	}

	/**
	 * 初始化配置信息
	 * 
	 * @param job jobconf
	 * @param tableName 表名
	 */
	public static void setOutput(Job job, String tableName) {
		job.setOutputFormatClass(HiveOutputFormat.class);
		job.setReduceSpeculativeExecution(false);
		HiveConfiguration dbConf = new HiveConfiguration(job.getConfiguration());
		dbConf.setOutputHiveTableName(tableName);
	}

	/**
	 * 执行ddlHQL语句
	 * 
	 * @param hiveConf hive配置
	 * @throws SQLException
	 */
	public static void executeDDLHQL(HiveConfiguration hiveConf) throws SQLException {
		String ddls = hiveConf.getOutputHiveExecuteDDLHQL();
		if (null == ddls || ddls.trim().length() <= 0) {
			return;
		}
		String ddl[] = ddls.split(";");
		Connection conn = null;
		try {
			conn = hiveConf.getOutputConnection();
		} catch (ClassNotFoundException e) {
			MRLog.error(LOG, "create hive conn error!");
			e.printStackTrace();
		}

		Statement stat = conn.createStatement();
		for (int i = 0; i < ddl.length; i++) {
			try {
				stat.executeQuery(ddl[i]);
			} catch (Exception e) {
				MRLog.errorException(LOG, "execute ddl error, hql:" + ddl[i], e);
			}
		}

		// 关闭连接
		close(conn);
	}

	/**
	 * 关闭连接
	 * 
	 * @param conn 连接对象
	 */
	public static void close(Connection conn) {
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				MRLog.error(LOG, "Close connection error!");
			}
		}
	}

	/**
	 * 执行ddlHQL语句
	 * 
	 * @param hiveConf hive配置
	 * @throws SQLException
	 */
	public static void executeDDLHQL(HiveConfiguration hiveConf, String ddl) throws SQLException {
		if (null == ddl || ddl.trim().length() <= 0) {
			return;
		}
		Connection conn = null;
		try {
			conn = hiveConf.getOutputConnection();
		} catch (ClassNotFoundException e) {
			MRLog.error(LOG, "create hive conn error!");
			e.printStackTrace();
		}

		Statement stat = conn.createStatement();
		try {
			stat.execute(ddl);
		} catch (Exception e) {
			MRLog.errorException(LOG, "execute ddl error, hql:" + ddl, e);
		}

		// 关闭连接
		close(conn);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException {

	}

	@Override
	public void handle(Job conf) throws Exception {
		/**
		 * 校验参数
		 */
		HiveConfiguration hconf = new HiveConfiguration(conf.getConfiguration());
		// 获取目标文件行分隔符
		String outRowChars = hconf.getOutputHiveFileRowsSplitChars();
		if (null == outRowChars || outRowChars.length() <= 0) {
			String meg = "输出行分隔符<" + HiveConfiguration.OUTPUT_HIVE_ROWS_SPLITCHARS + ">未设置";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 目标文件列分隔符
		String outFileSplitChars = hconf.getOutputHiveFileFieldSplitChars();
		if (null == outFileSplitChars || outFileSplitChars.trim().length() <= 0) {
			String meg = "输出行列隔符<" + HiveConfiguration.OUTPUT_HIVE_FIELD_SPLITCHARS + ">未设置";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		boolean para = hconf.getOutputHiveCompress();
		// 目标文件压缩方式 (压缩格式：HDFSUtils.CompressCodec)
		String outCompressCodec = hconf.getOutputHiveCompressCodec();
		if (para && !HDFSUtils.isExistCompressCodec(outCompressCodec)) {
			String meg = "[MR ERROR]目标文件压缩方式<" + HiveConfiguration.OUTPUT_HIVE_COMPRESS_CODEC + ">设置不正确.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 获取MR最终存放结果的目录
		String outTargetpath = hconf.getOutputTargetFilePath();
		hconf.setOutputTargetPath(outTargetpath);
		if (null == outTargetpath || outTargetpath.trim().length() <= 0) {
			MRLog.warn(LOG, "MR最终存放结果的目录<" + HiveConfiguration.OUTPUT_HIVE_TARGET_PATH + ">为空");
		}

		// 获取hive数据地址
		String hiveUrl = hconf.getOutPutHiveConfigUrl();
		if (null == hiveUrl || hiveUrl.trim().length() <= 0) {
			String meg = "[MR ERROR]Hive数据库地址<" + HiveConfiguration.OUTPUT_HIVE_CONFIG_URL + ">不能为空.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// hive数据库用户名
		String hiveUser = hconf.getOutPutHiveConfigUser();
		if (null == hiveUser || hiveUser.trim().length() <= 0) {
			LOG.warn("[MR WARN]hive数据库用户名<" + HiveConfiguration.OUTPUT_HIVE_CONFIG_USER + ">为空.");
		}

		// hive数据库密码
		String hivePwd = hconf.getOutPutHiveConfigPassword();
		if (null == hivePwd || hivePwd.trim().length() <= 0) {
			LOG.warn("[MR WARN]hive数据库密码<" + HiveConfiguration.OUTPUT_HIVE_CONFIG_PASSWORD + ">为空.");
		}

		// 目标表名
		String tableName = hconf.getOutputHiveTableName();
		if (null == tableName || tableName.trim().length() <= 0) {
			String meg = "[MR ERROR]Hive表名<" + HiveConfiguration.OUTPUT_TABLE + ">不能为空.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 验证分区参数
		String partitionField[] = hconf.getOutputHivePartitionField();
		if (null != partitionField && partitionField.length > 0) {
			// 目标字段
			String[] outputFieldName = hconf.getOutputFieldNames();
			if (null == outputFieldName || outputFieldName.length <= 0) {
				String meg = "目标字段<" + MRConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY + ">为空.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			for (int i = 0; i < partitionField.length; i++) {
				boolean isExist = false;
				for (String s : outputFieldName) {
					if (s.equals(partitionField[i])) {
						isExist = true;
						break;
					}
				}

				if (!isExist) {
					String meg = "分区字段" + partitionField[i] + "<" + HiveConfiguration.OUTPUT_HIVE_PARTITION_FIELD
							+ ">不存在<" + MRConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY + "中";
					MRLog.error(LOG, meg);
					throw new Exception(meg);
				}
			}

			String orderOutputTempPath = hconf.getOutputHiveOrderTempPath();
			if (null == orderOutputTempPath || orderOutputTempPath.trim().length() <= 0) {
				String meg = "临时输出目录<" + HiveConfiguration.OUTPUT_HIVE_ORDER_TEMP_PATH + ">为空.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			String orderOutputFileNamePrefix = hconf.getOutputHiveOrderFileNamePrefix();
			if (null == orderOutputFileNamePrefix || orderOutputFileNamePrefix.trim().length() <= 0) {
				String meg = "文件名称前缀<" + HiveConfiguration.OUTPUT_HIVE_ORDER_TEMP_PATH + ">为空.";
				MRLog.warn(LOG, meg);
			}

			long orderOutputFileMaxCount = hconf.getOutputHiveOrderFileMaxCount();
			if (orderOutputFileMaxCount == 0) {
				String meg = "文件记录的大小<" + HiveConfiguration.OUTPUT_HIVE_ORDER_FILEMAXCOUNT + ">必须大于0 或者-1(不限制).";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		// 执行的DDL语句
		String ddlHQL = hconf.getOutputHiveExecuteDDLHQL();
		if (null == ddlHQL || ddlHQL.trim().length() <= 0) {
			LOG.warn("[MR WARN]对hive数据库执行的<" + HiveConfiguration.OUTPUT_HIVE_DDL_HQL + ">未设置.");
		}

		try {
			executeDDLHQL(hconf);
			MRLog.info(LOG, "execute ddl hive sql success!");
		} catch (SQLException e) {
			MRLog.error(LOG, "execute ddl hive sql error!");
			e.printStackTrace();
		}

		conf.setReduceSpeculativeExecution(false);
		conf.setOutputFormatClass(HiveOutputFormat.class);
		conf.setOutputKeyClass(DBRecord.class);
		conf.setOutputValueClass(NullWritable.class);
		if (null != partitionField && partitionField.length > 0) {
			conf.setCombinerClass(DBGroupReducer.class);
			conf.setReducerClass(DBPartitionReducer.class);
		} else {
			conf.setCombinerClass(DBGroupReducer.class);
			conf.setReducerClass(DBReducer.class);
		}
	}
}
