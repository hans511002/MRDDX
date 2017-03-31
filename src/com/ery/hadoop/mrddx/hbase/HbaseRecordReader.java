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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.ery.hadoop.mrddx.hbase.HbaseInputFormat.HbaseInputSplit;
import com.ery.hadoop.mrddx.hbase.filter.HColumnCountGetFilter;
import com.ery.hadoop.mrddx.hbase.filter.HColumnPaginationFilter;
import com.ery.hadoop.mrddx.hbase.filter.HColumnPrefixFilter;
import com.ery.hadoop.mrddx.hbase.filter.HColumnRangeFilter;
import com.ery.hadoop.mrddx.hbase.filter.HDependentColumnFilter;
import com.ery.hadoop.mrddx.hbase.filter.HFamilyFilter;
import com.ery.hadoop.mrddx.hbase.filter.HFirstKeyOnlyFilter;
import com.ery.hadoop.mrddx.hbase.filter.HInclusiveStopFilter;
import com.ery.hadoop.mrddx.hbase.filter.HKeyOnlyFilter;
import com.ery.hadoop.mrddx.hbase.filter.HMultipleColumnPrefixFilter;
import com.ery.hadoop.mrddx.hbase.filter.HPageFilter;
import com.ery.hadoop.mrddx.hbase.filter.HPrefixFilter;
import com.ery.hadoop.mrddx.hbase.filter.HQualifierFilter;
import com.ery.hadoop.mrddx.hbase.filter.HRandomRowFilter;
import com.ery.hadoop.mrddx.hbase.filter.HRowFilter;
import com.ery.hadoop.mrddx.hbase.filter.HSingleColumnValueExcludeFilter;
import com.ery.hadoop.mrddx.hbase.filter.HSingleColumnValueFilter;
import com.ery.hadoop.mrddx.hbase.filter.HSkipFilter;
import com.ery.hadoop.mrddx.hbase.filter.HTimestampsFilter;
import com.ery.hadoop.mrddx.hbase.filter.HValueFilter;
import com.ery.hadoop.mrddx.hbase.filter.HWhileMatchFilter;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * A RecordReader that reads records from a SQL table. Emits LongWritables
 * containing the record number as key and DBWritables as value.
 * 



 * @createDate 2013-1-15
 * @version v1.0
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HbaseRecordReader<T extends HbaseWritable> extends RecordReader<LongWritable, T> {
	public static final Log LOG = LogFactory.getLog(HbaseRecordReader.class);

	HTable table = null;
	// HBase拆分对象
	private HbaseInputSplit inputSplit;

	// 处理输入结果的类
	private Class<T> inputClass;

	// conf
	private Configuration conf;

	// hbase conf
	private HbaseConfiguration dbConf;

	// 表名
	private String tableName;

	// 源字段与目标字段的对应关系,格式(family:column1 target1,target2)
	private Map<String, String[]> srcTargetFiledNameMap;

	// 扫描结果的类
	private ResultScanner resultScanner = null;

	// 当前位置
	private long pos = 0;

	// key对象
	private LongWritable key = null;

	// 处理输入结果的实例
	private T value = null;

	// 字段拆分符号
	private String splitSign;

	// 列簇与列的对应关系
	private String[][] clusterFieldNames;

	// 输入字段，与clusterFieldNames索引一致
	private String[][] inColumnSplitRelations;

	/**
	 * 构造函数
	 */
	public HbaseRecordReader() {
	}

	/**
	 * 构造方法
	 * 
	 * @param split 拆分对象
	 * @param inputClass value的class类型
	 * @param conf 系统配置
	 * @param dbConfig hbase系统配置
	 * @param tableName 表名
	 * @param fieldNames 字段名称数组
	 * @throws SQLException
	 */
	public HbaseRecordReader(HbaseInputFormat.HbaseInputSplit split, Class<T> inputClass, Configuration conf,
			HbaseConfiguration dbConfig, String tableName, String columnRelation) {
		this.srcTargetFiledNameMap = new Hashtable<String, String[]>();
		this.inputSplit = split;
		this.inputClass = inputClass;
		this.conf = conf;
		this.dbConf = dbConfig;
		this.tableName = tableName;
		this.splitSign = dbConf.getInputHBaseColumnSplitSign();
		List<String[]> list = new ArrayList<String[]>();// 获取列簇与列的对应关系
		List<String[]> rela = new ArrayList<String[]>();// 输入字段
		StringUtil.decodeOutColumnSplitRelation(columnRelation, list, rela);
		this.clusterFieldNames = list.toArray(new String[0][0]);
		this.inColumnSplitRelations = rela.toArray(new String[0][0]);

		for (int i = 0; i < this.clusterFieldNames.length; i++) {
			String stfn[] = this.clusterFieldNames[i];
			if (stfn.length == 2) {
				this.srcTargetFiledNameMap.put(stfn[0] + HbaseConfiguration.sign_lineae + stfn[1],
						this.inColumnSplitRelations[i]);
			}
		}
	}

	/**
	 * 执行查询
	 * 
	 * @return 查询结果
	 * @throws IOException IO异常
	 */
	protected ResultScanner executeQuery() throws IOException {
		table = new HTable(this.conf, this.tableName);
		Scan scan = new Scan();
		scan.setCacheBlocks(false);

		// 设置查询条件
		// TIMERANGE
		long timeRange[] = StringUtil.valueOfStringToLong(this.dbConf.getInputHBaseQueryTimerange());
		if (timeRange.length == 2 && timeRange[0] <= timeRange[1]) {
			scan.setTimeRange(timeRange[0], timeRange[1]);
		}

		// FILTER
		FilterList lstFilter = new FilterList();
		String filterContent = this.dbConf.getInputHBaseQueryFilters();
		if (null != filterContent && filterContent.length() > 0) {
			String filters[] = filterContent.split("-");
			for (int i = 0; i < filters.length; i++) {
				String tmp = StringUtil.decodeString(filters[i], "[", "]");
				if (null == filters[i]) {
					continue;
				}
				Map<String, String> mapFilterValue = StringUtil.valueOfStringToHashMap(tmp,
						HbaseConfiguration.sign_comma, HbaseConfiguration.sign_colon);
				Filter filter = this.getFilter(mapFilterValue);
				if (null != filter) {
					lstFilter.addFilter(filter);
				}
			}
		}

		if (lstFilter.getFilters().size() > 0) {
			scan.setFilter(lstFilter);
		}

		// FAMILYS
		String familys[] = this.dbConf.getInputHBaseQueryFamilys();
		if (null != familys) {
			for (int i = 0; i < familys.length; i++) {
				scan.addFamily(familys[i].getBytes());
			}
		}

		// FAMILYCOLUMNS
		String familyColumns[] = this.dbConf.getInputHBaseQueryFamilyColumns();
		if (null != familyColumns) {
			for (int i = 0; i < familyColumns.length; i++) {
				String fcolumn[] = familyColumns[i].split(HbaseConfiguration.sign_colon);
				scan.addColumn(fcolumn[0].getBytes(), fcolumn[1].getBytes());
			}
		}

		// 查询起始行到结束行, 左闭右开区间
		// STARTROW
		String startRow = this.inputSplit.getStart();
		if (null != startRow) {
			scan.setStartRow(Bytes.toBytes(startRow));
		}
		// STOPROW
		String stopRow = this.inputSplit.getEnd();
		if (null != startRow) {
			scan.setStopRow(Bytes.toBytes(stopRow));
		}

		// TIMESTAMP
		long timestamp = this.dbConf.getInputHBaseQueryTimestamp();
		if (timestamp != -1) {
			scan.setTimeStamp(timestamp);
		}

		// 打印过滤条件
		this.printScanConditions(scan);
		ResultScanner scaner = table.getScanner(scan);
		// table.close();
		return scaner;
	}

	/**
	 * 打印查询条件信息
	 * 
	 * @param scan 封装查询条件对象
	 */
	private void printScanConditions(Scan scan) {
		StringBuilder logMeg = new StringBuilder();
		logMeg.append("TimeRange==>");
		logMeg.append(scan.getTimeRange().getMin());
		logMeg.append(":");
		logMeg.append(scan.getTimeRange().getMax());
		logMeg.append("\n");
		logMeg.append("Filter==>");
		Filter filter = scan.getFilter();
		if (null != filter && filter instanceof FilterList) {
			List<Filter> filterLst = ((FilterList) filter).getFilters();
			for (Filter f : filterLst) {
				logMeg.append(f.getClass().getName());
				logMeg.append(":");
				logMeg.append(f.toString());
			}
		}
		logMeg.append("\n");
		logMeg.append("Family-Column==>");
		Map<byte[], NavigableSet<byte[]>> mapFamily = scan.getFamilyMap();
		Iterator<byte[]> iterator = mapFamily.keySet().iterator();
		while (iterator.hasNext()) {
			byte[] key = iterator.next();
			logMeg.append(new String(key));
			NavigableSet<byte[]> set = mapFamily.get(key);
			if (null == set) {
				continue;
			}
			Iterator<byte[]> setIterator = set.iterator();
			while (setIterator.hasNext()) {
				logMeg.append(new String(setIterator.next()));
				logMeg.append(",");
			}
		}
		logMeg.append("\n");
		logMeg.append("MaxVersions==>");
		logMeg.append(scan.getMaxVersions());
		logMeg.append("\n");
		logMeg.append("StartRow==>");
		logMeg.append(new String(scan.getStartRow()));
		logMeg.append("\n");
		logMeg.append("StopRow==>");
		logMeg.append(new String(scan.getStopRow()));
		MRLog.info(LOG, "[HBase scan conditions]\n" + logMeg.toString());
	}

	public LongWritable getCurrentKey() {
		return key;
	}

	public T getCurrentValue() {
		return value;
	}

	public LongWritable createKey() {
		return new LongWritable(0);
	}

	public T createValue() {
		return ReflectionUtils.newInstance(this.inputClass, this.conf);
	}

	public long getPos() throws IOException {
		return pos;
	}

	@Deprecated
	public boolean next(LongWritable key, T value) throws IOException {
		this.key = key;
		this.value = value;
		return this.nextKeyValue();
	}

	@Override
	public float getProgress() throws IOException {
		return 0.0f / Integer.MAX_VALUE;
	}

	/**
	 * @return
	 * @throws IOException
	 */
	public boolean nextKeyValue() throws IOException {
		if (this.key == null) {
			this.key = new LongWritable();
		}

		if (this.value == null) {
			this.value = this.createValue();
		}

		// 执行查询
		if (null == this.resultScanner) {
			MRLog.info(LOG, "开始执行查询语句");
			this.resultScanner = this.executeQuery();
		}

		Result result = this.resultScanner.next();
		if (null == result) {
			return false;
		}

		KeyValue keyValue[] = result.raw();
		this.value.readFields(keyValue, this.splitSign, this.srcTargetFiledNameMap);

		return true;
	}

	/**
	 * 一次性获取该行多版本的所有数据(暂时未使用)
	 * 
	 * @param keyValue keyvalue
	 */
	protected void getRowAllVersionData(KeyValue[] keyValue) {
		// 获取列族-列组合的个数
		Set<String> setFamilyColumn = new HashSet<String>();
		for (KeyValue kv : keyValue) {
			setFamilyColumn.add(new String(kv.getFamily()) + "-" + new String(kv.getQualifier()));
		}

		int columnCount = keyValue.length; // 列总数
		int fcCount = setFamilyColumn.size();// 列族与列组合的总数
		if (columnCount <= 0 || fcCount <= 0) {
			MRLog.warn(LOG, "读取数据为空!");
			return;
		}

		// 获取行记录
		int percount = columnCount / fcCount;
		for (int i = 0; i < percount; i++) {
			List<HBaseKeyValue> lst = new ArrayList<HBaseKeyValue>();
			for (int j = 0; j < percount * fcCount; j += percount) {
				HBaseKeyValue kv = new HBaseKeyValue();
				kv.setFamily(new String(keyValue[j + i].getFamily()));
				kv.setQualifier(new String(keyValue[j + i].getQualifier()));
				kv.setValue(new String(keyValue[j + i].getValue()));
				lst.add(kv);
			}
			// to do save record(lst)
		}
	}

	@Override
	public void close() throws IOException {
		if (null != this.resultScanner) {
			this.resultScanner.close();
		}
		try {
			if (table != null) {
				table.close();
			}
		} catch (Exception e) {
		}
	}

	/**
	 * 获取Filter
	 * 
	 * @param mapFilterValue 过滤类型与参数的对应关系
	 * @return 过滤对象
	 */
	public Filter getFilter(Map<String, String> mapFilterValue) {
		try {
			int type = Integer.parseInt(mapFilterValue.get(HbaseConfiguration.FILTERTYPE_NAME));
			return this.getFilter(type, mapFilterValue);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * 获取Filter
	 * 
	 * @param filterType (int) 过滤类型
	 * @param values 过滤参数
	 * @return
	 */
	public Filter getFilter(int filterType, Map<String, String> mapFilterValue) {
		switch (filterType) {
		case HbaseConfiguration.Filter_Type_ColumnCountGetFilter:
			return new HColumnCountGetFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_ColumnPaginationFilter:
			return new HColumnPaginationFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_ColumnPrefixFilter:
			return new HColumnPrefixFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_ColumnRangeFilter:
			return new HColumnRangeFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_DependentColumnFilter:
			return new HDependentColumnFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_FamilyFilter:
			return new HFamilyFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_QualifierFilter:
			return new HQualifierFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_RowFilter:
			return new HRowFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_ValueFilter:
			return new HValueFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_FirstKeyOnlyFilter:
			return new HFirstKeyOnlyFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_InclusiveStopFilter:
			return new HInclusiveStopFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_KeyOnlyFilter:
			return new HKeyOnlyFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_MultipleColumnPrefixFilter:
			return new HMultipleColumnPrefixFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_PageFilter:
			return new HPageFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_PrefixFilter:
			return new HPrefixFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_RandomRowFilter:
			return new HRandomRowFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_SingleColumnValueFilter:
			return new HSingleColumnValueFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_SingleColumnValueExcludeFilter:
			return new HSingleColumnValueExcludeFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_SkipFilter:
			return new HSkipFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_TimestampsFilter:
			return new HTimestampsFilter(mapFilterValue).getFilter();
		case HbaseConfiguration.Filter_Type_WhileMatchFilter:
			return new HWhileMatchFilter(mapFilterValue).getFilter();
		default:
			break;
		}
		return null;
	}

	/**
	 * 获取HBase的配置对象
	 * 
	 * @return HBase的配置对象
	 */
	protected HbaseConfiguration getDBConf() {
		return dbConf;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	}
}
