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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
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
import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * HBase的输入格式
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-4
 * @version v1.0
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HbaseInputFormat<T extends HbaseWritable> extends InputFormat<LongWritable, T> implements Configurable,
		IHandleFormat {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(HbaseInputFormat.class);

	/**
	 * 配置对象
	 */
	private HbaseConfiguration dbConf;

	public Configuration getConf() {
		return dbConf.getConf();
	}

	public HbaseConfiguration getDBConf() {
		return dbConf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.dbConf = new HbaseConfiguration(conf, HbaseConfiguration.FLAG_HBASE_INPUT);
	}

	/**
	 * Initializes the map-part of the job with the appropriate input settings.
	 * 
	 * @param job The map-reduce job
	 * @param inputClass 输入类型
	 * @param srcTargetFileNames 列与自定义字段
	 * @param tableName 表名
	 */
	public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName,
			String srcTargetFieldNames) {
		job.setInputFormatClass(HbaseInputFormat.class);
		HbaseConfiguration dbConf = new HbaseConfiguration(job.getConfiguration(), HbaseConfiguration.FLAG_HBASE_INPUT);
		dbConf.setInputClass(inputClass);
		dbConf.setInputTableName(tableName);
		dbConf.setInputHBaseColumnRelation(srcTargetFieldNames);
	}

	/**
	 * 设置查询条件 注：按需求设置查询条件，不是所有条件都需要设置值
	 * 
	 * @param timerange 时间范围(long)
	 * @param startrow 开始行
	 * @param stoprow 结束行
	 * @param timestamp 时间戳
	 * @param filters 过滤器
	 * @param familyColumns 列族和列
	 * @param familys 列族
	 */
	public static void setInputQueryCondition(Configuration job, long[] timerange, String startrow, String stoprow,
			long timestamp, String[] filters, String[] familyColumns, String[] familys) {
		HbaseConfiguration dbConf = new HbaseConfiguration(job, HbaseConfiguration.FLAG_HBASE_INPUT);
		dbConf.setInputHBaseQueryTimerange(StringUtil.valueOfLongToString(timerange));
		dbConf.setInputHBaseQueryStartRow(startrow);
		dbConf.setInputHBaseQueryStopRow(stoprow);
		dbConf.setInputHBaseQueryTimestamp(timestamp);
		dbConf.setInputHBaseQueryFilters(filters);
		dbConf.setInputHBaseQueryFamilyColumns(familyColumns);
		dbConf.setInputHBaseQueryFamilys(familys);
	}

	/**
	 * 获取表的RegionInfo的数量
	 * 
	 * @param job job
	 * @return 表的RegionInfo的数量
	 * @throws Exception 异常
	 */
	public static int getTableHRegionInfoCount(Configuration job, String startKey, String endKey) throws Exception {
		HbaseConfiguration dbConf = new HbaseConfiguration(job, HbaseConfiguration.FLAG_HBASE_INPUT);
		String tableName = dbConf.getInputTableName();
		if (null == tableName) {
			String meg = "The name of table is null!";
			MRLog.error(LOG, meg);
			throw new IOException(meg);
		}

		if (!validateCondition(dbConf, dbConf.getInputTableName())) {
			String meg = "validate condition error!";
			MRLog.error(LOG, meg);
			throw new IOException(meg);
		}

		List<HRegionLocation> lstHRegionLocation = getTableHRegionInfo(job, tableName, startKey, endKey);
		if (null == lstHRegionLocation || lstHRegionLocation.size() <= 0) {
			String meg = "The account of table'regionInfo is zero!";
			MRLog.error(LOG, meg);
			throw new IOException(meg);
		}

		// 打印Table所有的RegionInfo信息
		printTableAllRegionInfo(job, tableName);
		printTableRequestRegionInfo(lstHRegionLocation, tableName);

		return lstHRegionLocation.size();
	}

	/**
	 * 获取表的RegionInfo列表
	 * 
	 * @param job job
	 * @param tableName 表名
	 * @param startKey 开始rowkey
	 * @param endKey 结束rowkey
	 * @return 表的RegionInfo的列表
	 * @throws Exception 异常
	 */
	public static List<HRegionLocation> getTableHRegionInfo(Configuration job, String tableName, String startKey,
			String endKey) throws IOException {
		// 获取startkey,endkey
		HTable table = new HTable(job, tableName);
		byte[][] startKeys = table.getStartKeys();
		byte[][] endKeys = table.getEndKeys();

		// 1：获取表所有的regionlocation
		if (null == startKey && null == endKey) {
			return table.getRegionsInRange(startKeys[0], endKeys[endKeys.length - 1]);
		}

		// 2: 获取表指定范围的regionlocation
		if (startKey == null) {
			startKey = new String(startKeys[0]);
		}

		if (endKey == null) {
			endKey = new String(endKeys[endKeys.length - 1]);
		}

		// 获取实际的HRegionInfo列表
		List<HRegionLocation> lsHRegionInfo = table.getRegionsInRange(startKey.getBytes(), endKey.getBytes());
		return lsHRegionInfo;
	}

	/**
	 * 检查查询条件参数
	 * 
	 * @param dbConf 配置信息
	 * @param tableName 表名
	 * @return true 表示参数正确
	 * @throws TableNotFoundException 表不存在
	 * @throws IOException
	 */
	protected static boolean validateCondition(HbaseConfiguration dbConf, String tableName)
			throws TableNotFoundException, IOException {
		if (null == tableName) {
			return false;
		}

		HBaseAdmin admin = new HBaseAdmin(dbConf.getConf());
		HTableDescriptor tableDes = admin.getTableDescriptor(tableName.getBytes());
		// 列族名称列表
		Set<byte[]> setByte = tableDes.getFamiliesKeys();
		Set<String> familySets = new HashSet<String>();
		Iterator<byte[]> iterator = setByte.iterator();
		while (iterator.hasNext()) {
			familySets.add(new String(iterator.next()));
		}

		// check column and family
		String familyColumns[] = dbConf.getInputHBaseQueryFamilyColumns();
		if (null != familyColumns) {
			for (int i = 0; i < familyColumns.length; i++) {
				if (null == familyColumns[i] || familyColumns[i].trim().length() <= 0) {
					String meg = "The parameter of columnfamily is null!";
					MRLog.error(LOG, meg);
					return false;
				}

				String fcolumn[] = familyColumns[i].split(HbaseConfiguration.sign_colon);
				if (fcolumn.length != 2) {
					String meg = "The parameter of columnfamily is format error !";
					MRLog.error(LOG, meg);
					return false;
				}

				// check column
				HColumnDescriptor hcDesc = tableDes.getFamily(fcolumn[0].getBytes());
				if (!(null != hcDesc && familySets.contains(fcolumn[0]) && fcolumn[0].equals(new String(hcDesc
						.getName())))) {
					String meg = "Column is not exist! column:" + fcolumn[0];
					MRLog.error(LOG, meg);
					return false;
				}
			}
		}

		// check family
		String familys[] = dbConf.getInputHBaseQueryFamilys();
		if (null != familys) {
			for (int i = 0; i < familys.length; i++) {
				if (!familySets.contains(familys[i])) {
					String meg = "Family is not exist! family:" + familys[i];
					MRLog.error(LOG, meg);
					return false;
				}
			}
		}

		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Class<T> inputClass = (Class<T>) (this.dbConf.getInputClass());
		return new HbaseRecordReader<T>((HbaseInputSplit) split, inputClass, this.getConf(), this.getDBConf(),
				this.dbConf.getInputTableName(), this.dbConf.getInputHBaseColumnRelation());
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {

		String startRow = this.dbConf.getInputHBaseQueryStartRow();
		String stopRow = this.dbConf.getInputHBaseQueryStopRow();
		String tableName = this.dbConf.getInputTableName();

		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<HRegionLocation> lstHRegionLocation = getTableHRegionInfo(job.getConfiguration(), tableName, startRow,
				stopRow);
		for (int i = 0; i < lstHRegionLocation.size(); i++) {
			HRegionLocation hRegionLocation = lstHRegionLocation.get(i);
			String tempStart = null;
			String tempEnd = null;
			HRegionInfo hRegionInfo = hRegionLocation.getRegionInfo();
			// NO.1：未指定起始行和结束行
			if (null == startRow && null == stopRow) {
				tempStart = new String(hRegionInfo.getStartKey());
				tempEnd = new String(hRegionInfo.getEndKey());
				HbaseInputSplit split = new HbaseInputSplit(tempStart, tempEnd);
				splits.add(split);
				continue;
			}

			// NO.2：起始行和结束行，至少指定了一个
			byte[] startKeyByte = hRegionInfo.getStartKey();
			byte[] endKeyByte = hRegionInfo.getEndKey();
			if (null != startRow && hRegionInfo.containsRow(startRow.getBytes())) {
				tempStart = startRow;
			}

			if (null != stopRow && hRegionInfo.containsRow(stopRow.getBytes())) {
				tempEnd = stopRow;
			}

			tempStart = tempStart != null ? tempStart : new String(startKeyByte);
			tempEnd = tempEnd != null ? tempEnd : new String(endKeyByte);
			HbaseInputSplit split = new HbaseInputSplit(tempStart, tempEnd);
			splits.add(split);
		}

		MRLog.info(LOG, "Finished hbase split!");
		return splits;
	}

	/**
	 * 打印Table所有的RegionInfo信息
	 * 
	 * @param job job对象
	 * @param tableName 表名
	 * @throws IOException IO异常
	 */
	private static void printTableAllRegionInfo(Configuration job, String tableName) throws IOException {
		HTable table = new HTable(job, tableName);
		StringBuilder regionLog = new StringBuilder();
		regionLog.append("<<Table[");
		regionLog.append(new String(table.getTableName()));
		regionLog.append("]all RegionInfo>>");
		NavigableMap<HRegionInfo, ServerName> mapHRegionInfo = table.getRegionLocations();
		Iterator<HRegionInfo> hregionIterator = mapHRegionInfo.keySet().iterator();
		while (hregionIterator.hasNext()) {
			regionLog.append("\nHRegionInfo:");
			HRegionInfo key = hregionIterator.next();
			ServerName value = mapHRegionInfo.get(key);
			regionLog.append(key.toString());
			regionLog.append("ServerInfo:");
			regionLog.append("{name=>:");
			regionLog.append(value.getServerName());
			regionLog.append(" ,HostAndPort=>:");
			regionLog.append(value.getHostAndPort());
			regionLog.append("}");
		}
		MRLog.info(LOG, regionLog.toString());
	}

	/**
	 * 打印需要请求Table所有的RegionInfo信息
	 * 
	 * @param lstHRegionInfo HRegionInfo列表
	 * @param tableName 表名
	 */
	private static void printTableRequestRegionInfo(List<HRegionLocation> lstHRegionInfo, String tableName) {
		StringBuilder regionLog = new StringBuilder();
		regionLog.append("<<Table[");
		regionLog.append(tableName);
		regionLog.append("]request RegionInfo>>");
		for (HRegionLocation hRegionLocation : lstHRegionInfo) {
			regionLog.append("\nHRegionInfo:");
			if (null == hRegionLocation) {
				MRLog.warn(LOG, "获取需要的HRegionLocation存在为null的情况!");
				continue;
			}
			HRegionInfo hRegionInfo = hRegionLocation.getRegionInfo();
			if (null == hRegionInfo) {
				MRLog.warn(LOG, "获取需要的HRegionInfo存在为null的情况!");
				continue;
			}
			regionLog.append(hRegionInfo.toString());
			regionLog.append("ServerInfo:");
			regionLog.append("{HostAndPort=>:");
			regionLog.append(hRegionLocation.getHostnamePort());
			regionLog.append("}");
		}
		MRLog.info(LOG, regionLog.toString());
	}

	/**
	 * A InputSplit that spans a set of rows
	 */
	public static class HbaseInputSplit extends InputSplit implements Writable {
		private String start;
		private String end;

		public HbaseInputSplit() {
		}

		/**
		 * Convenience Constructor
		 * 
		 * @param start the index of the first row to select
		 * @param end the index of the last row to select
		 */
		public HbaseInputSplit(String start, String end) {
			this.start = start;
			this.end = end;
			MRLog.info(LOG, "HBase Split rowkey range=>" + this.start + ":" + this.end);
		}

		public String getStart() {
			return start;
		}

		public String getEnd() {
			return end;
		}

		@Override
		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		@Override
		public long getLength() throws IOException {
			return 0;
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			this.start = input.readUTF();
			this.end = input.readUTF();
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeUTF(this.start);
			output.writeUTF(this.end);
		}
	}

	/**
	 * 获取源字段
	 * 
	 * @param paraMap 参数
	 * @return 输入字段
	 * @throws Exception
	 */
	protected String getParamInputFieldNames(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputFieldNames");
		if (null == para || para.trim().length() <= 0) {
			String meg = "源字段<inputFieldNames>为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	@Override
	public void handle(Job conf) throws Exception {
		// HBase表名称
		HbaseConfiguration hconf = new HbaseConfiguration(conf.getConfiguration(), HbaseConfiguration.FLAG_HBASE_INPUT);
		String tableName = hconf.getInputTableName();
		if (null == tableName || tableName.trim().length() <= 0) {
			String meg = "[MR ERROR]HBase表名称<" + HbaseConfiguration.INPUT_TABLE + ">不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 列与源字段对应关系
		String inputFieldName[] = hconf.getInputFieldNames();
		this.vParamSrcTargetFieldNames(hconf, inputFieldName);

		if (hconf.getInputIsCombiner()) {
			conf.setCombinerClass(DBGroupReducer.class);
		}

		// 查询条件：TIMERANGE
		String timerange[] = hconf.getInputHBaseQueryTimerange();
		this.vParamQueryTimeRange(timerange);

		// 查询条件：startrow
		String startrow = hconf.getInputHBaseQueryStartRow();
		if (null == startrow || startrow.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的startrow<" + HbaseConfiguration.INPUT_QUERY_STARTROW + ">未设置.");
		}

		// 查询条件：stoprow
		String stoprow = hconf.getInputHBaseQueryStopRow();
		if (null == stoprow || stoprow.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的stoprow<" + HbaseConfiguration.INPUT_QUERY_STOPROW + ">未设置.");
		}

		// 查询条件：timestamp
		long timestamp = hconf.getInputHBaseQueryTimestamp();
		if (timestamp <= -1) {
			MRLog.warn(LOG, "[MR WARN]查询条件的时间戳<" + HbaseConfiguration.INPUT_QUERY_TIMESTAMP + ">未设置.");
		}

		// 查询条件：filters
		String filters = hconf.getInputHBaseQueryFilters();
		if (null == filters || filters.length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的过滤条件<" + HbaseConfiguration.INPUT_QUERY_FILTER + ">未设置.");
		}

		// 查询条件：familyColumns
		String familyColumns[] = hconf.getInputHBaseQueryFamilyColumns();
		if (null == familyColumns || familyColumns.length <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的列<" + HbaseConfiguration.INPUT_QUERY_FAMILYCOLUMNS + ">未设置.");
		}

		if (null != familyColumns) {
			for (String tmp : familyColumns) {
				if (tmp.split(":").length != 2) {
					String meg = "[MR ERROR]查询条件的列<" + HbaseConfiguration.INPUT_QUERY_FAMILYCOLUMNS + ">设置错误.";
					MRLog.error(LOG, meg);
					throw new Exception(meg);
				}
			}
		}

		// 查询条件：familys
		String familys[] = hconf.getInputHBaseQueryFamilys();
		if (null == familys || familys.length <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的列族<" + HbaseConfiguration.INPUT_QUERY_FAMILYS + ">未设置.");
		}

		conf.setInputFormatClass(HbaseInputFormat.class);
		hconf.setInputClass(DBRecord.class);

		// 获取MapTask的数量
		int taskNumber = HbaseInputFormat.getTableHRegionInfoCount(conf.getConfiguration(), startrow, stoprow);
		int reduceTasks = taskNumber;
		if (hconf.getInputMapEnd()) {
			reduceTasks = 0;
		}

		// 设置任务数
		hconf.setNumMapTasks(taskNumber);
		hconf.setNumReduceTasks(reduceTasks);
		hconf.setInputClass(DBRecord.class);
		conf.setMapperClass(DBMapper.class);
		conf.setMapOutputKeyClass(DBRecord.class);
		conf.setMapOutputValueClass(DBRecord.class);
		if (hconf.getInputIsCombiner()) {
			conf.setCombinerClass(DBGroupReducer.class);
		}
	}

	/**
	 * 获取列与自定义源字段
	 * 
	 * @param hconf
	 * 
	 * @param hFieldName hbase输入字段
	 * @param inputFieldName 输入字段
	 * @return
	 * @throws Exception
	 */
	private void vParamSrcTargetFieldNames(HbaseConfiguration hconf, String[] inputFieldName) throws Exception {
		// 检查拆分字段是否包含在输出字段中
		String inColumnRelation = hconf.getInputHBaseColumnRelation();
		if (null == inColumnRelation) {
			String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.INPUT_HBASE_COLUMN_RELATION + ">不能为空";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		List<String[]> list = new ArrayList<String[]>();// 获取列簇与列的对应关系
		List<String[]> rela = new ArrayList<String[]>();// 输入字段
		StringUtil.decodeOutColumnSplitRelation(inColumnRelation, list, rela);
		String[][] clusterFieldNames = list.toArray(new String[0][0]);
		String[][] inColumnSplitRelations = rela.toArray(new String[0][0]);
		if (clusterFieldNames.length <= 0 || inColumnSplitRelations.length <= 0) {
			String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.INPUT_HBASE_COLUMN_RELATION + ">不正确";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 验证拆分列字段
		String splitSign = hconf.getInputHBaseColumnSplitSign();
		Set<String> setSrcFiled = StringUtil.parseStringArrayToSet(inputFieldName);
		for (int i = 0; i < inColumnSplitRelations.length; i++) {
			String temp[] = inColumnSplitRelations[i];
			for (int j = 0; j < temp.length; j++) {
				if (!setSrcFiled.contains(temp[j])) {
					String meg = "[MR ERROR]<" + HbaseConfiguration.INPUT_HBASE_COLUMN_RELATION + ">字段中列不在输入字段<"
							+ HbaseConfiguration.SYS_INPUT_FIELD_NAMES_PROPERTY + ">中";
					LOG.error(meg);
					throw new Exception(meg);
				}
			}

			if (temp.length > 1 && (null == splitSign || splitSign.trim().length() <= 0)) {
				String meg = "[MR ERROR]<" + HbaseConfiguration.INPUT_HBASE_COLUMN_RELATION
						+ ">字段中存在输入字段数量大于1, 需要设置分隔符<" + HbaseConfiguration.INPUT_HBASE_COLUMN_SPLIT_SIGN + ">";
				LOG.error(meg);
				throw new Exception(meg);
			}
		}
	}

	/**
	 * 获取查询条件的时间范围
	 * 
	 * @param timerange 时间范围
	 * @throws Exception 异常
	 */
	private void vParamQueryTimeRange(String[] timerange) throws Exception {
		if (null == timerange || timerange.length <= 0) {
			// 不包含在查询条件中.
			MRLog.warn(LOG, "[MR WARN]查询条件的时间范围<" + HbaseConfiguration.INPUT_QUERY_TIMERANGE + ">未设置.");
			return;
		}

		if (timerange.length != 2) {
			String meg = "[MR ERROR]HBase查询条件的时间范围<" + HbaseConfiguration.INPUT_QUERY_TIMERANGE
					+ ">设置错误,比如:132342155,32423532.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		long trange[] = new long[2];
		try {
			trange[0] = Long.parseLong(timerange[0]);
			trange[1] = Long.parseLong(timerange[1]);
		} catch (Exception e) {
			String meg = "[MR ERROR]HBase查询条件的时间范围<" + HbaseConfiguration.INPUT_QUERY_TIMERANGE + ">设置错误,可能为空或不是数字.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (trange[0] > trange[1]) {
			String meg = "[MR ERROR]HBase查询条件的时间范围<" + HbaseConfiguration.INPUT_QUERY_TIMERANGE + ">设置错误,起始时间大于结束时间.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
	}

}