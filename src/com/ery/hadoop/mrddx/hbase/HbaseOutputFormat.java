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

package com.ery.hadoop.mrddx.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-15
 * @version v1.0
 * @param <K>
 * @param <V>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HbaseOutputFormat<K extends HbaseWritable, V> extends OutputFormat<K, NullWritable> implements
		IHandleFormat {
	private static final Log LOG = LogFactory.getLog(HbaseOutputFormat.class);

	/**
	 * 验证表
	 * 
	 * @param dbConf
	 *            hbase配置
	 * @return true表示验证成功
	 * @throws Exception
	 *             异常
	 */
	public static boolean validateTable(HbaseConfiguration dbConf) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		dbConf.setOutputHBaseConfParam(conf);
		HBaseAdmin admin = new HBaseAdmin(conf);

		// 判断表是否存在
		String tableName = dbConf.getOutputHBaseTableName();
		boolean isExistTable = admin.tableExists(tableName);
		if (!isExistTable) {
			// 创建表
			return createTable(dbConf);
		}

		if (!dbConf.getOutputHBaseTableIsModify()) {
			return true;
		}

		// 获取参数
		int blocksize = dbConf.getOutputHBaseColmunBlocksize();
		Algorithm compactionCompressionType = dbConf.getOutputHBaseColmunCompactionCompressionType();
		Algorithm compressionType = dbConf.getOutputHBaseColmunCompressionType();
		int maxversion = dbConf.getOutputHBaseColmunMaxversion();
		int minversion = dbConf.getOutputHBaseColmunMinversion();
		BloomType bloomType = dbConf.getOutputHBaseBloomFilterType();
		boolean inMemory = dbConf.getOutputHBaseSetInMemory();

		// 获取表结构信息
		HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(tableName));

		// 取出需要修改的添加和修改的列族
		List<HColumnDescriptor> lstModifyHColumnDesc = new ArrayList<HColumnDescriptor>();
		List<String> lstFamily = Arrays.asList(dbConf.getOutputHBaseFamilyNames());
		List<String> lstExistFamily = new ArrayList<String>();
		for (HColumnDescriptor hcd : tableDesc.getColumnFamilies()) {
			if (lstFamily.contains(hcd.getNameAsString())) {
				lstExistFamily.add(hcd.getNameAsString());
			}

			if (blocksize != hcd.getBlocksize()
					|| !compactionCompressionType.equals(hcd.getCompactionCompressionType())
					|| !compressionType.equals(hcd.getCompressionType()) || maxversion != (hcd.getMaxVersions())
					|| minversion != (hcd.getMinVersions()) || bloomType != hcd.getBloomFilterType()
					|| inMemory != hcd.isInMemory()) {
				lstModifyHColumnDesc.add(hcd);
			}
		}

		// 禁用表
		if (admin.isTableEnabled(tableName)) {
			admin.disableTable(tableName);
		}

		// 添加列族
		boolean isAddColumn = false;
		for (String familyName : lstFamily) {
			if (lstExistFamily.contains(familyName)) {
				continue;
			}
			isAddColumn = true;
			HColumnDescriptor hcolumn = new HColumnDescriptor(familyName);
			hcolumn.setBlocksize(blocksize);
			hcolumn.setCompactionCompressionType(compactionCompressionType);
			hcolumn.setCompressionType(compressionType);
			hcolumn.setMinVersions(maxversion);
			hcolumn.setMaxVersions(minversion);
			hcolumn.setBloomFilterType(bloomType);
			hcolumn.setInMemory(inMemory);
			admin.addColumn(tableName, hcolumn);
		}

		if (isAddColumn) {
			MRLog.info(LOG, "新增列成功!");
		}

		// 修改列
		for (HColumnDescriptor hcolumn : lstModifyHColumnDesc) {
			hcolumn.setBlocksize(blocksize);
			hcolumn.setCompactionCompressionType(compactionCompressionType);
			hcolumn.setCompressionType(compressionType);
			hcolumn.setMinVersions(maxversion);
			hcolumn.setMaxVersions(minversion);
			hcolumn.setBloomFilterType(bloomType);
			hcolumn.setInMemory(inMemory);
			admin.modifyColumn(tableName, hcolumn);
		}

		if (lstModifyHColumnDesc.size() > 0) {
			MRLog.info(LOG, "修改列成功!");
		}

		// 启用表
		if (!admin.isTableEnabled(tableName)) {
			admin.enableTable(tableName);
		}

		return true;
	}

	/**
	 * 创建表
	 * 
	 * @param dbConf
	 *            hbase config
	 * @return true 创建成功，否则，失败
	 * @throws Exception
	 *             异常
	 */
	public static boolean createTable(HbaseConfiguration dbConf) throws Exception {
		// 获取参数
		String tableName = dbConf.getOutputHBaseTableName();
		int blocksize = dbConf.getOutputHBaseColmunBlocksize();
		Algorithm compactionCompressionType = dbConf.getOutputHBaseColmunCompactionCompressionType();
		Algorithm compressionType = dbConf.getOutputHBaseColmunCompressionType();
		int maxversion = dbConf.getOutputHBaseColmunMaxversion();
		int minversion = dbConf.getOutputHBaseColmunMinversion();
		BloomType bloomType = dbConf.getOutputHBaseBloomFilterType();
		boolean inMemory = dbConf.getOutputHBaseSetInMemory();

		long hfileMaxfilesize = dbConf.getOutputHBaseHFileMaxfilesize();
		long memstoreFlushSize = dbConf.getOutputHBaseMemstoreFlushSize();
		boolean isDeferredLogFlush = dbConf.getOutputHBaseIsDeferredLogFlush();
		String familyNames[] = dbConf.getOutputHBaseFamilyNames();

		// 创建表
		try {
			HBaseAdmin admin = new HBaseAdmin(dbConf.getConf());
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.setMaxFileSize(hfileMaxfilesize);
			tableDesc.setMemStoreFlushSize(memstoreFlushSize);
			tableDesc.setDeferredLogFlush(isDeferredLogFlush);

			// 添加列族
			for (int i = 0; i < familyNames.length; i++) {
				HColumnDescriptor hcolumn = new HColumnDescriptor(familyNames[i]);
				hcolumn.setBlocksize(blocksize);
				hcolumn.setCompactionCompressionType(compactionCompressionType);
				hcolumn.setCompressionType(compressionType);
				hcolumn.setMinVersions(minversion);
				hcolumn.setMaxVersions(maxversion);
				hcolumn.setBloomFilterType(bloomType);
				hcolumn.setInMemory(inMemory);
				tableDesc.addFamily(hcolumn);
			}
			admin.createTable(tableDesc);
			MRLog.info(LOG, "Create hbase table success!");
			return true;
		} catch (IOException e) {
			String errorInfo = "Create hbase table error!";
			MRLog.error(LOG, errorInfo);
			throw new Exception(errorInfo);
		}
	}

	@Override
	public RecordWriter<K, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
			InterruptedException {
		// public RecordWriter<K, NullWritable> getRecordWriter(FileSystem
		// ignored, JobConf job, String name,
		// Progressable progress) throws IOException {
		HbaseConfiguration dbConf = new HbaseConfiguration(context.getConfiguration(),
				HbaseConfiguration.FLAG_HBASE_OUTPUT);
		HTable table = new HTable(dbConf.getConf(), dbConf.getOutputHBaseTableName());
		table.setAutoFlush(false, true);
		table.setWriteBufferSize(dbConf.getOutputHBaseWriteBuffersize());
		return new HbaseRecordWriter<K, NullWritable>(dbConf, table);
	}

	/**
	 * 初始化输出到hbase的参数设置
	 * 
	 * @param job
	 *            配置对象
	 * @param tableName
	 *            表名
	 * @param rowKeyRule
	 *            行规则
	 * @param memstoreFlushSize
	 *            缓存大小
	 * @param isDeferredLogFlush
	 *            日志刷新
	 * @param hfileMaxfilesize
	 *            hfile文件的最大值
	 * @param colmunCompressionType
	 *            列压缩类型
	 * @param colmunCompactionCompressionType
	 *            列合并压缩类型
	 * @param colmunBlocksize
	 *            列块大小
	 * @param colmunMaxversion
	 *            列的最大版本
	 * @param colmunMinversion
	 *            列的最小版本
	 * @param bloomType
	 *            过滤类型
	 * @param inMemory
	 *            是否缓存
	 * @param commitBufferLength
	 *            提交缓存大小
	 * @param familyColumnName
	 *            列族名称
	 * @return HbaseConfiguration HBase配置对象
	 * @throws Exception
	 *             异常
	 */
	public static HbaseConfiguration setOutput(Job job, String tableName, String rowKeyRule, long memstoreFlushSize,
			boolean isDeferredLogFlush, int hfileMaxfilesize, int colmunCompressionType,
			int colmunCompactionCompressionType, int colmunBlocksize, int colmunMaxversion, int colmunMinversion,
			int bloomType, boolean inMemory, int commitBufferLength, String familyColumnName) throws Exception {
		HbaseConfiguration dbConf = setOutput(job, tableName);
		if (null != familyColumnName) {
			String familyColumnNames[] = familyColumnName.split(",");
			// 获取family列表
			List<String> lstFamily = new ArrayList<String>();
			for (int i = 0; i < familyColumnNames.length; i++) {
				if (null == familyColumnNames[i]) {
					String errorInfo = "Parameter Execption:" + familyColumnNames;
					MRLog.error(LOG, errorInfo);
					throw new Exception(errorInfo);
				}

				String temp[] = familyColumnNames[i].split(HbaseConfiguration.sign_colon);
				if (temp.length != 2 || temp[0].trim().length() <= 0 || temp[1].trim().length() <= 0) {
					String errorInfo = "Parameter Execption:" + familyColumnNames;
					MRLog.error(LOG, errorInfo);
					throw new Exception(errorInfo);
				}

				if (!lstFamily.contains(temp[0])) {
					lstFamily.add(temp[0]);
				}
			}

			dbConf.setOutputHBaseFamilyNames(lstFamily.toArray(new String[0]));
			dbConf.setOutputHBaseFieldNames(familyColumnName);
		}

		dbConf.setOutputHBaseRowKeyRule(rowKeyRule);

		dbConf.setOutputHBaseHFileMaxfilesize(hfileMaxfilesize);
		dbConf.setOutputHBaseMemstoreFlushSize(memstoreFlushSize);
		dbConf.setOutputHBaseIsDeferredLogFlush(isDeferredLogFlush);

		dbConf.setOutputHBaseColmunCompressionType(colmunCompressionType);
		dbConf.setOutputHBaseColmunCompactionCompressionType(colmunCompactionCompressionType);
		dbConf.setOutputHBaseColmunBlocksize(colmunBlocksize);
		dbConf.setOutputHBaseColmunMaxversion(colmunMaxversion);
		dbConf.setOutputHBaseColmunMinversion(colmunMinversion);
		dbConf.setOutputHBaseBloomFilterType(bloomType);
		dbConf.setOutputHBaseSetInMemory(inMemory);
		dbConf.setOutputHBaseBufferLength(commitBufferLength);

		// 检查表
		if (!validateTable(dbConf)) {
			String errorInfo = "HBase output table, validate Execption!";
			MRLog.error(LOG, errorInfo);
			throw new Exception(errorInfo);
		}

		// 打印表信息
		printTableDesc(tableName, dbConf.getConf());
		return dbConf;
	}

	/**
	 * 初始化输出到hbase的参数设置
	 * 
	 * @param job
	 *            配置对象
	 * @param tableName
	 *            表名
	 * @param colmunCompressionType
	 *            列压缩类型
	 * @param colmunCompactionCompressionType
	 *            列合并压缩类型
	 * @param colmunBlocksize
	 *            列块大小
	 * @param colmunMaxversion
	 *            列的最大版本
	 * @param colmunMinversion
	 *            列的最小版本
	 * @param bloomType
	 *            过滤类型
	 * @param inMemory
	 *            是否缓存
	 * @param familyColumnName
	 *            列族名称
	 * @return HbaseConfiguration HBase配置对象
	 * @throws Exception
	 *             异常
	 */
	public static HbaseConfiguration setOutput(Job job, String tableName, int colmunCompressionType,
			int colmunCompactionCompressionType, int colmunBlocksize, int colmunMaxversion, int colmunMinversion,
			int bloomType, boolean inMemory, String familyColumnName) throws Exception {
		HbaseConfiguration dbConf = setOutput(job, tableName);
		if (null != familyColumnName) {
			String familyColumnNames[] = familyColumnName.split(",");
			// 设置family列表
			List<String> lstFamily = new ArrayList<String>();
			for (int i = 0; i < familyColumnNames.length; i++) {
				if (null == familyColumnNames[i]) {
					String errorInfo = "Parameter Execption:" + familyColumnNames;
					MRLog.error(LOG, errorInfo);
					throw new Exception(errorInfo);
				}

				String temp[] = familyColumnNames[i].split(HbaseConfiguration.sign_colon);
				if (temp.length != 2 || temp[0].trim().length() <= 0 || temp[1].trim().length() <= 0) {
					String errorInfo = "Parameter Execption:" + familyColumnNames;
					MRLog.error(LOG, errorInfo);
					throw new Exception(errorInfo);
				}

				if (!lstFamily.contains(temp[0])) {
					lstFamily.add(temp[0]);
				}
			}

			dbConf.setOutputHBaseFamilyNames(lstFamily.toArray(new String[0]));
			dbConf.setOutputHBaseFieldNames(familyColumnName);
		}

		dbConf.setOutputHBaseColmunCompressionType(colmunCompressionType);
		dbConf.setOutputHBaseColmunCompactionCompressionType(colmunCompactionCompressionType);
		dbConf.setOutputHBaseColmunBlocksize(colmunBlocksize);
		dbConf.setOutputHBaseColmunMaxversion(colmunMaxversion);
		dbConf.setOutputHBaseColmunMinversion(colmunMinversion);
		dbConf.setOutputHBaseBloomFilterType(bloomType);
		dbConf.setOutputHBaseSetInMemory(inMemory);

		// 检查表
		if (validateTable(dbConf)) {
			String errorInfo = "validateTable Execption!";
			MRLog.error(LOG, errorInfo);
			throw new Exception(errorInfo);
		}

		return dbConf;
	}

	/**
	 * 设置format等参数
	 * 
	 * @param job
	 *            配置对象
	 * @param tableName
	 *            表名
	 * @return HbaseConfiguration HBase配置对象
	 * @throws IOException
	 *             IO异常
	 */
	public static HbaseConfiguration setOutput(Job job, String tableName) {
		job.setOutputFormatClass(HbaseOutputFormat.class);
		job.setReduceSpeculativeExecution(false);
		HbaseConfiguration dbConf = new HbaseConfiguration(job.getConfiguration(), HbaseConfiguration.FLAG_HBASE_OUTPUT);
		dbConf.setOutputHBaseTableName(tableName);
		return dbConf;
	}

	/**
	 * 打印表结构信息
	 * 
	 * @param tableName
	 *            表名
	 * @param conf
	 *            配置对象
	 * @throws TableNotFoundException
	 *             表不存在异常
	 * @throws IOException
	 *             IO异常
	 */
	public static void printTableDesc(String tableName, Configuration conf) throws TableNotFoundException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor tableDes = admin.getTableDescriptor(Bytes.toBytes(tableName));

		// table属性
		StringBuilder s = new StringBuilder();
		Map<ImmutableBytesWritable, ImmutableBytesWritable> values = tableDes.getValues();
		for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : values.entrySet()) {
			String key = Bytes.toString(e.getKey().get());
			String value = Bytes.toString(e.getValue().get());
			s.append(", ");
			s.append(key);
			s.append(" => '");
			s.append(value);
			s.append("'");
		}
		MRLog.info(LOG, "TableName:" + tableName + ",属性:" + s.toString());

		// 列族信息
		HColumnDescriptor hColumnDescriptor[] = tableDes.getColumnFamilies();
		for (HColumnDescriptor hcd : hColumnDescriptor) {
			MRLog.info(LOG, "列族:" + hcd.toString());
		}
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handle(Job conf) throws Exception {
		HbaseConfiguration hConf = new HbaseConfiguration(conf.getConfiguration(), HbaseConfiguration.FLAG_HBASE_OUTPUT);

		// 输出表名
		String tableName = hConf.getOutputHBaseTableName();
		if (null == tableName || tableName.trim().length() <= 0) {
			String meg = "HBase输出表名<" + HbaseConfiguration.OUTPUT_TABLE + ">不能为空.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 列族列表
		String hbaseFieldNames = hConf.getOutputHBaseFieldNames();
		this.vParamTargetFamilyNames(hbaseFieldNames, hConf);
		hConf.setOutputHBaseFamilyNames(this.getHBaseFamilyNames(hbaseFieldNames));

		// 行规则
		String rowKeyRule = hConf.getOutputHBaseRowKeyRule();
		if (null == rowKeyRule || rowKeyRule.trim().length() <= 0) {
			String meg = "表的行规则<" + HbaseConfiguration.OUTPUT_ROWKEY_RULE + ">未设置";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// HFile的最大值
		long hfileMaxfilesize = hConf.getOutputHBaseHFileMaxfilesize();
		if (hfileMaxfilesize <= 0) {
			String meg = "HFile的最大值<" + HbaseConfiguration.OUTPUT_HFILE_MAXFILESIZE + ">必须大于0";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 设置memstore flush到HDFS上的文件大小
		long memstoreFlushSize = hConf.getOutputHBaseMemstoreFlushSize();
		if (memstoreFlushSize <= 0) {
			String meg = "设置memstore flush到HDFS上的文件大小<" + HbaseConfiguration.OUTPUT_MEMSTORE_FLUSHSIZE + ">不能小于0";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 列块大小
		int colmunBlocksize = hConf.getOutputHBaseColmunBlocksize();
		if (colmunBlocksize <= 0) {
			String meg = "列块大小<" + HbaseConfiguration.OUTPUT_COLMUN_BLOCKSIZE + ">必须大于0";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 最大版本号
		int colmunMaxversion = hConf.getOutputHBaseColmunMaxversion();
		if (colmunMaxversion <= 0) {
			String meg = "最大版本号<" + HbaseConfiguration.OUTPUT_COLMUN_MAXVERSION + ">必须大于0";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 最小版本号
		int colmunMinversion = hConf.getOutputHBaseColmunMinversion();
		if (colmunMinversion <= 0) {
			String meg = "最小版本号<" + HbaseConfiguration.OUTPUT_COLMUN_MINVERSION + ">必须大于0";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 提交数据的缓存大小
		int commitBufferLength = hConf.getOutputHBaseBufferLength();
		if (commitBufferLength <= 0) {
			String meg = "提交数据的缓存大小<" + HbaseConfiguration.OUTPUT_SET_COMMIT_BUFFERLENGTH + ">必须大于0";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 是否在写入hbase时，设置WAL日志功能
		int walFlag = hConf.getOutputHBaseSetWalFlags();
		if (!(walFlag == -1 || walFlag >= 0 || walFlag <= 4)) {
			String meg = "WAL<" + HbaseConfiguration.OUTPUT_SET_WAL_FLAG + ">不正确，默认为-1表示不设置，取值范围:[0-4]";
			LOG.error(meg);
			throw new Exception(meg);
		}

		// 检查表
		if (!validateTable(hConf)) {
			String errorInfo = "HBase output table, validate Execption!";
			MRLog.error(LOG, errorInfo);
			throw new Exception(errorInfo);
		}

		conf.setOutputFormatClass(HbaseOutputFormat.class);
		conf.setReduceSpeculativeExecution(false);
		conf.setOutputKeyClass(DBRecord.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setReducerClass(DBReducer.class);

		// 打印表信息
		printTableDesc(tableName, hConf.getConf());
	}

	public String[] getHBaseFamilyNames(String familyColumnName) throws Exception {
		Set<String> lstFamily = new HashSet<String>();
		if (null != familyColumnName) {
			String familyColumnNames[] = familyColumnName.split(",");
			// 获取family列表
			for (int i = 0; i < familyColumnNames.length; i++) {
				if (null == familyColumnNames[i]) {
					String errorInfo = "Parameter Execption:" + familyColumnNames;
					MRLog.error(LOG, errorInfo);
					throw new Exception(errorInfo);
				}

				String temp[] = familyColumnNames[i].split(HbaseConfiguration.sign_colon);
				if (temp.length != 2 || temp[0].trim().length() <= 0 || temp[1].trim().length() <= 0) {
					String errorInfo = "Parameter Execption:" + familyColumnNames;
					MRLog.error(LOG, errorInfo);
					throw new Exception(errorInfo);
				}

				if (!lstFamily.contains(temp[0])) {
					lstFamily.add(temp[0]);
				}
			}
		}

		return lstFamily.toArray(new String[0]);
	}

	/**
	 * 验证目标列族和列的对应关系
	 * 
	 * @param hConf
	 *            配置对象
	 * @param paraMap
	 *            参数
	 * @throws Exception
	 *             异常
	 */
	private void vParamTargetFamilyNames(String hbaseFieldNames, HbaseConfiguration hConf) throws Exception {
		// 验证列簇与列字段
		if (null == hbaseFieldNames || hbaseFieldNames.trim().length() <= 0) {
			String meg = "[MR ERROR]目标列族和列的对应关系<" + HbaseConfiguration.OUTPUT_COLMUN_FAMILYS_REL + ">未设置.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		String famliyNames[] = hbaseFieldNames.split(",");
		Set<String> tempVar = new HashSet<String>();
		for (String fn : famliyNames) {
			String fc[] = fn.split(":");
			if (tempVar.contains(fn)) {
				String meg = "[MR ERROR]目标列族和列的对应关系<" + HbaseConfiguration.OUTPUT_COLMUN_FAMILYS_REL + ">设置错误.不能有重复";
				LOG.error(meg);
				throw new Exception(meg);
			}

			tempVar.add(fn);
			if (fc.length != 2 || fc[0].trim().length() <= 0 || fc[1].trim().length() <= 0) {
				String meg = "[MR ERROR]目标列族和列的对应关系<" + HbaseConfiguration.OUTPUT_COLMUN_FAMILYS_REL + ">设置错误.";
				LOG.error(meg);
				throw new Exception(meg);
			}
		}

		// 检查拆分字段是否包含在输出字段中
		String outColumnRelation = hConf.getOutputHBaseColumnRelation();
		if (null == outColumnRelation) {
			String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">不能为空";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		List<String[]> list = new ArrayList<String[]>();// 获取列簇与列的对应关系
		List<String[]> rela = new ArrayList<String[]>();
		StringUtil.decodeOutColumnSplitRelation(outColumnRelation, list, rela);
		String[][] clusterFieldNames = list.toArray(new String[0][0]);
		String[][] outColumnSplitRelations = rela.toArray(new String[0][0]);
		if (clusterFieldNames.length <= 0 || outColumnSplitRelations.length <= 0) {
			String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">不正确";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 验证拆分列簇与列字段
		for (int i = 0; i < clusterFieldNames.length; i++) {
			String tmp = clusterFieldNames[i][0] + ":" + clusterFieldNames[i][1];
			if (!tempVar.contains(tmp)) {
				String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">的列簇与列不在<"
						+ HbaseConfiguration.OUTPUT_COLMUN_FAMILYS_REL + ">列表中";
				LOG.error(meg);
				throw new Exception(meg);
			}
		}

		// 验证拆分列字段
		String splitSign = hConf.getOutputHBaseColumnSplitSign();
		Set<String> setTargetFiled = StringUtil.parseStringArrayToSet(hConf.getOutputFieldNames());
		for (int i = 0; i < outColumnSplitRelations.length; i++) {
			String temp[] = outColumnSplitRelations[i];
			for (int j = 0; j < temp.length; j++) {
				if (!setTargetFiled.contains(temp[j])) {
					String meg = "[MR ERROR]<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">字段中列不在输出字段<"
							+ HbaseConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY + ">中";
					LOG.error(meg);
					throw new Exception(meg);
				}
			}

			if (temp.length <= 0) {
				String meg = "[MR ERROR]<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">字段中存在不包含输出字段的列簇与列的关系";
				LOG.error(meg);
				throw new Exception(meg);
			}

			if (temp.length > 1 && (null == splitSign || splitSign.trim().length() <= 0)) {
				String meg = "[MR ERROR]<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION
						+ ">字段中存在输出字段数量大于1, 需要设置分隔符<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_SPLIT_SIGN + ">";
				LOG.error(meg);
				throw new Exception(meg);
			}
		}
	}

	OutputCommitter committer = null;

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		if (committer == null) {
			committer = new TableOutputCommitter();
		}
		return committer;
	}
}