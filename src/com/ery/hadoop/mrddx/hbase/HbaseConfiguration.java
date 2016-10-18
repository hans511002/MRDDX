package com.ery.hadoop.mrddx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

import com.ery.hadoop.mrddx.MRConfiguration;

/**
 * HBase的配置信息类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * @author wanghao
 * @createDate 2013-1-15
 * @version v1.0
 */
public class HbaseConfiguration extends MRConfiguration {
	/**
	 * 压缩类型(Compression.Algorithm中的类型对应)
	 */
	public static final int COMPRESSION_ALGORITHM_NONE = 0;
	public static final int COMPRESSION_ALGORITHM_LZO = 1;
	public static final int COMPRESSION_ALGORITHM_GZ = 2;
	public static final int COMPRESSION_ALGORITHM_SNAPPY = 3;

	/**
	 * bloomTye(StoreFile.BloomType中的类型对应)
	 */
	public static final int BloomType_NONE = 0;
	public static final int BloomType_ROW = 1;
	public static final int BloomType_ROWCOL = 2;

	/**
	 * Filter 类型(int)
	 */
	public static final int Filter_Type_ColumnCountGetFilter = 1;
	public static final int Filter_Type_ColumnPaginationFilter = 2;
	public static final int Filter_Type_ColumnPrefixFilter = 3;
	public static final int Filter_Type_ColumnRangeFilter = 4;
	public static final int Filter_Type_DependentColumnFilter = 5;
	public static final int Filter_Type_FamilyFilter = 6;
	public static final int Filter_Type_QualifierFilter = 7;
	public static final int Filter_Type_RowFilter = 8;
	public static final int Filter_Type_ValueFilter = 9;
	public static final int Filter_Type_FirstKeyOnlyFilter = 10;
	public static final int Filter_Type_InclusiveStopFilter = 11;
	public static final int Filter_Type_KeyOnlyFilter = 12;
	public static final int Filter_Type_MultipleColumnPrefixFilter = 13;
	public static final int Filter_Type_PageFilter = 14;
	public static final int Filter_Type_PrefixFilter = 15;
	public static final int Filter_Type_RandomRowFilter = 16;
	public static final int Filter_Type_SingleColumnValueFilter = 17;
	public static final int Filter_Type_SingleColumnValueExcludeFilter = 18;
	public static final int Filter_Type_SkipFilter = 19;
	public static final int Filter_Type_TimestampsFilter = 20;
	public static final int Filter_Type_WhileMatchFilter = 21;

	// 常量
	public static final String FILTERTYPE_NAME = "filterType"; // filter类型的名称
	public static final String sign_comma = ",";// 逗号
	public static final String sign_colon = ":";// 冒号
	public static final String sign_lineae = "-";// 横线
	public static final String sign_semicolon = ";";// 分号

	// 是否修改表结构(0:表示不修改)
	public static final int TABLE_ISMODIFY_MODIFY = 0;

	// 输入标示符
	public static final int FLAG_HBASE_INPUT = 0;
	// 输出标示符
	public static final int FLAG_HBASE_OUTPUT = 1;

	/**
	 * 输入参数配置
	 */
	// hbase数据源
	// 单次查询缓存数量
	public static final String INPUT_HBASE_SOURCE_CLIENT_SCANNER_CACHING = "input.mr.mapred.hbase.client.scanner.caching";
	// 集群zk列表
	public static final String INPUT_HBASE_SOURCE_ZOOKEEPER_QUORUM = "input.mr.mapred.hbase.zookeeper.quorum";
	// zk hbase的根路径
	public static final String INPUT_HBASE_SOURCE_ZOOKEEPER_ZNODE_PARENT = "input.mr.mapred.hbase.zookeeper.znode.parent";
	// zk 端口
	public static final String INPUT_HBASE_SOURCE_ZOOKEEPER_CLIENTPORT = "input.mr.mapred.hbase.zookeeper.property.clientPort";
	// 表名称
	public static final String INPUT_TABLE = "input.mr.maperd.hbase.table";
	// 输入字段拆分符号
	public static final String INPUT_HBASE_COLUMN_SPLIT_SIGN = "input.mr.maperd.hbase.column.split.sign";
	// 输入字段拆分字段关系,例如：[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]],列的值保持顺序
	public static final String INPUT_HBASE_COLUMN_RELATION = "input.mr.maperd.hbase.column.relation";
	// 查询条件：TIMERANGE
	public static final String INPUT_QUERY_TIMERANGE = "input.mr.maperd.hbase.table.query.timerange";
	// 查询条件：FILTER
	public static final String INPUT_QUERY_FILTER = "input.mr.maperd.hbase.table.query.filter";
	// 查询条件：FAMILYCOLUMNS
	public static final String INPUT_QUERY_FAMILYCOLUMNS = "input.mr.maperd.hbase.table.query.familycolumns";
	// 查询条件：FAMILYS
	public static final String INPUT_QUERY_FAMILYS = "input.mr.maperd.hbase.table.query.familys";
	// 查询条件：STARTROW
	public static final String INPUT_QUERY_STARTROW = "input.mr.maperd.hbase.table.query.startrow";
	// 查询条件：STOPROW
	public static final String INPUT_QUERY_STOPROW = "input.mr.maperd.hbase.table.query.stoprow";
	// 查询条件：TIMESTAMP
	public static final String INPUT_QUERY_TIMESTAMP = "input.mr.maperd.hbase.table.query.timestamp";

	/**
	 * 输出参数配置
	 */
	// 单次查询缓存数量
	public static final String OUTPUT_HBASE_SOURCE_CLIENT_SCANNER_CACHING = "output.mr.mapred.hbase.client.scanner.caching";
	// 集群zk列表
	public static final String OUTPUT_HBASE_SOURCE_ZOOKEEPER_QUORUM = "output.mr.mapred.hbase.zookeeper.quorum";
	// zk hbase的根路径
	public static final String OUTPUT_HBASE_SOURCE_ZOOKEEPER_ZNODE_PARENT = "output.mr.mapred.hbase.zookeeper.znode.parent";
	// zk 端口
	public static final String OUTPUT_HBASE_SOURCE_ZOOKEEPER_CLIENTPORT = "output.mr.mapred.hbase.zookeeper.property.clientPort";
	// 表名
	public static final String OUTPUT_TABLE = "output.mr.maperd.hbase.table";
	// 写数据的大小(单位:b)
	public static final String OUTPUT_WRITE_BUFFERSIZE = "output.mr.maperd.write.buffersize";
	// 是否修改表结构(0:表示不修改)
	public static final String OUTPUT_TABLE_ISMODIFY = "output.mr.maperd.hbase.table.ismodify";
	// rowKey 规则
	public static final String OUTPUT_ROWKEY_RULE = "output.mr.maperd.hbase.table.rowkeyrule";
	// familys（通过OUTPUT_COLMUN_FAMILYS_REL获取）
	public static final String OUTPUT_COLMUN_FAMILYS = "output.mr.maperd.hbase.table.familys";
	// 列簇:列 例如：system:cpu
	public static final String OUTPUT_COLMUN_FAMILYS_REL = "output.mr.maperd.hbase.table.columnfamily.rels";
	// 列的压缩类型
	public static final String OUTPUT_COLMUN_COMPRESSIONTYPE = "output.mr.maperd.hbase.colmun.compressiontype";
	// 列的合并压缩类型
	public static final String OUTPUT_COLMUN_COMPACTION_COMPRESSIONTYPE = "output.mr.maperd.hbase.colmun.compactioncompressiontype";
	// Hfile的最大值
	public static final String OUTPUT_HFILE_MAXFILESIZE = "output.mr.maperd.hbase.hfile.maxfilesize";
	// Memstore flush到HDFS上的文件大小
	public static final String OUTPUT_MEMSTORE_FLUSHSIZE = "output.mr.maperd.hbase.memstore.flushsize";
	// 设置是否延迟同步到文件系统的日志
	public static final String OUTPUT_ISDEFERREDLOG_FLUSH = "output.mr.maperd.hbase.isdeferredlog.flush";
	// 列的块大小
	public static final String OUTPUT_COLMUN_BLOCKSIZE = "output.mr.maperd.hbase.colmun.blocksize";
	// 列的最大版本号
	public static final String OUTPUT_COLMUN_MAXVERSION = "output.mr.maperd.hbase.colmun.maxversion";
	// 列的最小版本号
	public static final String OUTPUT_COLMUN_MINVERSION = "output.mr.maperd.hbase.colmun.minversion";
	// bloomtype
	public static final String OUTPUT_BLOOMFILTERTYPE = "output.mr.maperd.hbase.bloomfiltertype";
	// 添加到HBase中的新数据，是否保存在内存中
	public static final String OUTPUT_SET_INMEMORY = "output.mr.maperd.hbase.set.inmemory";
	// 提交数据的缓存大小
	public static final String OUTPUT_SET_COMMIT_BUFFERLENGTH = "output.mr.maperd.hbase.set.commit.bufferlength";
	// 删除数据标示符
	public static final String OUTPUT_DATA_DELETE_FLAG = "output.mr.maperd.hbase.data.delete.flag";
	// 输出字段拆分符号
	public static final String OUTPUT_HBASE_COLUMN_SPLIT_SIGN = "output.mr.maperd.hbase.column.split.sign";
	// 输出字段拆分字段关系,例如：[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]]-[列簇:列名称:[a,b,c]],列的值保持顺序
	public static final String OUTPUT_HBASE_COLUMN_RELATION = "output.mr.maperd.hbase.column.relation";
	// WAL日志标示符[0-4]
	public static final String OUTPUT_SET_WAL_FLAG = "output.mr.maperd.hbase.wal.flag";
	// 是否使用hbase的唯一rowkey标示符(true：使用)位数：19位，0000001385698716616
	public static final String OUTPUT_HBASE_USE_ONLYROWKEY = "output.mr.maperd.hbase.use.onlyrowkey";
	// blukload输出参数,例如：hbase_home目录下的 hbase-0.94.11.jar
	public static final String OUTPUT_HBASE_JAR_NAME = "output.mr.maperd.hbase.hbasejar.name";

	/**
	 * 系统配置
	 */
	private Configuration conf;

	/**
	 * 构造方法
	 * 
	 * @param job
	 *            配置对象
	 * @param flag
	 *            0:输入,1:输出
	 */
	public HbaseConfiguration(Configuration job, int flag) {
		super(job);
		this.conf = job;
		switch (flag) {
		case 0:
			this.conf.set("hbase.zookeeper.quorum", this.getInputHBaseZKQuorum());
			this.conf.set("zookeeper.znode.parent", this.getInputHBaseZKNodeParent());
			this.conf.setInt("hbase.zookeeper.property.clientPort", this.getInputHBaseZKClientPort());
			this.conf.setLong("hbase.client.scanner.caching", this.getInputHBaseClientScannerCaching());
			break;
		case 1:
			this.conf.set("hbase.zookeeper.quorum", this.getOutputHBaseZKQuorum());
			this.conf.set("zookeeper.znode.parent", this.getOutputHBaseZKNodeParent());
			this.conf.setInt("hbase.zookeeper.property.clientPort", this.getOutputHBaseZKClientPort());
			this.conf.setLong("hbase.client.scanner.caching", this.getOutputHBaseClientScannerCaching());
			break;
		default:
		}
	}

	public Configuration getConf() {
		return conf;
	}

	/**
	 * 设置输出配置参数(hbase运行必须的)
	 * 
	 * @param conf
	 */
	public void setOutputHBaseConfParam(Configuration conf) {
		conf.set("hbase.zookeeper.quorum", this.getOutputHBaseZKQuorum());
		conf.set("zookeeper.znode.parent", this.getOutputHBaseZKNodeParent());
		conf.setInt("hbase.zookeeper.property.clientPort", this.getOutputHBaseZKClientPort());
		conf.setLong("hbase.client.scanner.caching", this.getOutputHBaseClientScannerCaching());
	}

	public void setInputHBaseClientScannerCaching(long cachNum) {
		this.conf.setLong(HbaseConfiguration.INPUT_HBASE_SOURCE_CLIENT_SCANNER_CACHING, cachNum);
	}

	public long getInputHBaseClientScannerCaching() {
		return this.conf.getLong(HbaseConfiguration.INPUT_HBASE_SOURCE_CLIENT_SCANNER_CACHING, 1000);
	}

	public void setInputHBaseZKQuorum(String zkQuorum) {
		this.conf.set(HbaseConfiguration.INPUT_HBASE_SOURCE_ZOOKEEPER_QUORUM, zkQuorum);
	}

	public String getInputHBaseZKQuorum() {
		return this.conf.get(HbaseConfiguration.INPUT_HBASE_SOURCE_ZOOKEEPER_QUORUM, "");
	}

	public void setInputHBaseZKNodeParent(String zkNodeParent) {
		this.conf.set(HbaseConfiguration.INPUT_HBASE_SOURCE_ZOOKEEPER_ZNODE_PARENT, zkNodeParent);
	}

	public String getInputHBaseZKNodeParent() {
		return this.conf.get(HbaseConfiguration.INPUT_HBASE_SOURCE_ZOOKEEPER_ZNODE_PARENT, "");
	}

	public void setInputHBaseZKClientPort(int zkClientPort) {
		this.conf.setInt(HbaseConfiguration.INPUT_HBASE_SOURCE_ZOOKEEPER_CLIENTPORT, zkClientPort);
	}

	public int getInputHBaseZKClientPort() {
		return this.conf.getInt(HbaseConfiguration.INPUT_HBASE_SOURCE_ZOOKEEPER_CLIENTPORT, 2181);
	}

	public void setInputTableName(String tableName) {
		this.conf.set(HbaseConfiguration.INPUT_TABLE, tableName);
	}

	public String getInputTableName() {
		return this.conf.get(HbaseConfiguration.INPUT_TABLE);
	}

	public void setInputHBaseColumnSplitSign(String splitSign) {
		this.conf.set(HbaseConfiguration.INPUT_HBASE_COLUMN_SPLIT_SIGN, splitSign);
	}

	public String getInputHBaseColumnSplitSign() {
		return this.conf.get(HbaseConfiguration.INPUT_HBASE_COLUMN_SPLIT_SIGN, null);
	}

	public void setInputHBaseColumnRelation(String relation) {
		this.conf.set(HbaseConfiguration.INPUT_HBASE_COLUMN_RELATION, relation);
	}

	public String getInputHBaseColumnRelation() {
		return this.conf.get(HbaseConfiguration.INPUT_HBASE_COLUMN_RELATION, null);
	}

	/**
	 * 设置输入参数(查询条件：TIMERANGE)
	 * 
	 * @param timerange
	 *            []
	 */
	public void setInputHBaseQueryTimerange(String... timerange) {
		if (null == timerange) {
			return;
		}
		this.conf.setStrings(HbaseConfiguration.INPUT_QUERY_TIMERANGE, timerange);
	}

	public String[] getInputHBaseQueryTimerange() {
		return this.conf.getStrings(HbaseConfiguration.INPUT_QUERY_TIMERANGE);
	}

	/**
	 * 设置输入参数(查询条件：FILTER) 格式：filterType:1,limit:1000,... 以SingleColumnValueFilter为例
	 * filterType:17,family:f1,qualifier:c1,CompareOp:EQUAL,value:r3cf1c1value1
	 * 
	 * @param filter
	 *            []
	 */
	public void setInputHBaseQueryFilters(String... filter) {
		if (null == filter) {
			return;
		}
		this.conf.setStrings(HbaseConfiguration.INPUT_QUERY_FILTER, filter);
	}

	public String getInputHBaseQueryFilters() {
		return this.conf.get(HbaseConfiguration.INPUT_QUERY_FILTER, null);
	}

	/**
	 * 设置输入参数(查询条件：familyColumns)
	 * 
	 * @param familyColumns
	 */
	public void setInputHBaseQueryFamilyColumns(String[] familyColumns) {
		if (null == familyColumns) {
			return;
		}
		this.conf.setStrings(HbaseConfiguration.INPUT_QUERY_FAMILYCOLUMNS, familyColumns);
	}

	public String[] getInputHBaseQueryFamilyColumns() {
		return this.conf.getStrings(HbaseConfiguration.INPUT_QUERY_FAMILYCOLUMNS, new String[0]);
	}

	/**
	 * 设置输入参数(查询条件：familys)
	 * 
	 * @param familys
	 */
	public void setInputHBaseQueryFamilys(String[] familys) {
		if (null == familys) {
			return;
		}
		this.conf.setStrings(HbaseConfiguration.INPUT_QUERY_FAMILYS, familys);
	}

	public String[] getInputHBaseQueryFamilys() {
		return this.conf.getStrings(HbaseConfiguration.INPUT_QUERY_FAMILYS, new String[0]);
	}

	/**
	 * 设置输入参数(查询条件：STARTROW)
	 * 
	 * @param startRow
	 */
	public void setInputHBaseQueryStartRow(String startRow) {
		if (null == startRow) {
			return;
		}

		this.conf.set(HbaseConfiguration.INPUT_QUERY_STARTROW, startRow);
	}

	public String getInputHBaseQueryStartRow() {
		return this.conf.get(HbaseConfiguration.INPUT_QUERY_STARTROW, null);
	}

	/**
	 * 设置输入参数(查询条件：STOPROW)
	 * 
	 * @param stopRow
	 */
	public void setInputHBaseQueryStopRow(String stopRow) {
		if (null == stopRow) {
			return;
		}

		this.conf.set(HbaseConfiguration.INPUT_QUERY_STOPROW, stopRow);
	}

	public String getInputHBaseQueryStopRow() {
		return this.conf.get(HbaseConfiguration.INPUT_QUERY_STOPROW, null);
	}

	/**
	 * 设置输入参数(查询条件：TIMESTAMP)
	 * 
	 * @param timestamp
	 */
	public void setInputHBaseQueryTimestamp(long timestamp) {
		this.conf.setLong(HbaseConfiguration.INPUT_QUERY_TIMESTAMP, timestamp);
	}

	public long getInputHBaseQueryTimestamp() {
		return this.conf.getLong(HbaseConfiguration.INPUT_QUERY_TIMESTAMP, -1);
	}

	public void setOutputHBaseClientScannerCaching(long cachNum) {
		this.conf.setLong(HbaseConfiguration.OUTPUT_HBASE_SOURCE_CLIENT_SCANNER_CACHING, cachNum);
	}

	public long getOutputHBaseClientScannerCaching() {
		return this.conf.getLong(HbaseConfiguration.OUTPUT_HBASE_SOURCE_CLIENT_SCANNER_CACHING, 1000);
	}

	public void setOutputHBaseZKQuorum(String zkQuorum) {
		this.conf.set(HbaseConfiguration.OUTPUT_HBASE_SOURCE_ZOOKEEPER_QUORUM, zkQuorum);
	}

	public String getOutputHBaseZKQuorum() {
		return this.conf.get(HbaseConfiguration.OUTPUT_HBASE_SOURCE_ZOOKEEPER_QUORUM, "");
	}

	public void setOutputHBaseZKNodeParent(String zkNodeParent) {
		this.conf.set(HbaseConfiguration.OUTPUT_HBASE_SOURCE_ZOOKEEPER_ZNODE_PARENT, zkNodeParent);
	}

	public String getOutputHBaseZKNodeParent() {
		return this.conf.get(HbaseConfiguration.OUTPUT_HBASE_SOURCE_ZOOKEEPER_ZNODE_PARENT, "");
	}

	public void setOutputHBaseZKClientPort(int zkClientPort) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_HBASE_SOURCE_ZOOKEEPER_CLIENTPORT, zkClientPort);
	}

	public int getOutputHBaseZKClientPort() {
		return this.conf.getInt(HbaseConfiguration.OUTPUT_HBASE_SOURCE_ZOOKEEPER_CLIENTPORT, 2181);
	}

	public void setOutputHBaseTableName(String tableName) {
		this.conf.set(HbaseConfiguration.OUTPUT_TABLE, tableName);
	}

	public String getOutputHBaseTableName() {
		return this.conf.get(HbaseConfiguration.OUTPUT_TABLE);
	}

	public void setOutputHBaseWriteBuffersize(long bufferSize) {
		this.conf.setLong(HbaseConfiguration.OUTPUT_WRITE_BUFFERSIZE, bufferSize);
	}

	public long getOutputHBaseWriteBuffersize() {
		return this.conf.getLong(HbaseConfiguration.OUTPUT_WRITE_BUFFERSIZE, 10485760);
	}

	public void setOutputHBaseTableIsModify(int modify) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_TABLE_ISMODIFY, modify);
	}

	public boolean getOutputHBaseTableIsModify() {
		int modify = this.conf.getInt(HbaseConfiguration.OUTPUT_TABLE_ISMODIFY, 0);
		if (TABLE_ISMODIFY_MODIFY == modify) {
			return false;
		}

		return true;
	}

	/**
	 * 设置rowkey规则 例如：tj-{USER_ID}-{CREATE_TIME}-{DATE,yyyy-mm-dd} 说明：{XXXXX}:完全替换目标字段的内容，{xxxxx, 替换格式}:按照"替换格式"替换目标字段，其余字符不变
	 * 
	 * @param fieldNames
	 */
	public void setOutputHBaseRowKeyRule(String rowKeyRule) {
		this.conf.setStrings(HbaseConfiguration.OUTPUT_ROWKEY_RULE, rowKeyRule);
	}

	public String getOutputHBaseRowKeyRule() {
		return this.conf.get(HbaseConfiguration.OUTPUT_ROWKEY_RULE);
	}

	/**
	 * 设置family名称列表
	 * 
	 * @param fieldNames
	 */
	public void setOutputHBaseFamilyNames(String... fieldNames) {
		this.conf.setStrings(HbaseConfiguration.OUTPUT_COLMUN_FAMILYS, fieldNames);
	}

	public String[] getOutputHBaseFamilyNames() {
		return this.conf.getStrings(HbaseConfiguration.OUTPUT_COLMUN_FAMILYS);
	}

	/**
	 * 设置family和colnum的名称列表
	 */
	public void setOutputHBaseFieldNames(String fieldNames) {
		this.conf.set(HbaseConfiguration.OUTPUT_COLMUN_FAMILYS_REL, fieldNames);
	}

	public String getOutputHBaseFieldNames() {
		return this.conf.get(HbaseConfiguration.OUTPUT_COLMUN_FAMILYS_REL, "");
	}

	/**
	 * 列的压缩类型
	 * 
	 * @param type
	 */
	public void setOutputHBaseColmunCompressionType(int type) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_COLMUN_COMPRESSIONTYPE, type);
	}

	public Algorithm getOutputHBaseColmunCompressionType() {
		int compressionType = this.conf.getInt(HbaseConfiguration.OUTPUT_COLMUN_COMPRESSIONTYPE, COMPRESSION_ALGORITHM_NONE);
		return getHBaseCompressionType(compressionType);
	}

	/**
	 * 列的合并压缩类型
	 * 
	 * @param type
	 */
	public void setOutputHBaseColmunCompactionCompressionType(int type) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_COLMUN_COMPACTION_COMPRESSIONTYPE, type);
	}

	public Algorithm getOutputHBaseColmunCompactionCompressionType() {
		int compressionType = this.conf.getInt(HbaseConfiguration.OUTPUT_COLMUN_COMPACTION_COMPRESSIONTYPE, COMPRESSION_ALGORITHM_NONE);
		return getHBaseCompressionType(compressionType);
	}

	/**
	 * Hfile的最大值（单位:字节）
	 * 
	 * @param size
	 */
	public void setOutputHBaseHFileMaxfilesize(long size) {
		this.conf.setLong(HbaseConfiguration.OUTPUT_HFILE_MAXFILESIZE, size);
	}

	public long getOutputHBaseHFileMaxfilesize() {
		// 默认256MB
		return this.conf.getLong(HbaseConfiguration.OUTPUT_HFILE_MAXFILESIZE, 256 * 1024 * 1024);
	}

	/**
	 * 设置memstore flush到HDFS上的文件大小
	 * 
	 * @param memstoreFlushSize
	 *            大小
	 */
	public void setOutputHBaseMemstoreFlushSize(long memstoreFlushSize) {
		this.conf.setLong(HbaseConfiguration.OUTPUT_MEMSTORE_FLUSHSIZE, memstoreFlushSize);
	}

	public long getOutputHBaseMemstoreFlushSize() {
		// defaults 64 MB
		return this.conf.getLong(HbaseConfiguration.OUTPUT_MEMSTORE_FLUSHSIZE, 64 * 1024 * 1024);
	}

	/**
	 * 设置延迟同步到文件系统的日志
	 * 
	 * @param isFlush
	 */
	public void setOutputHBaseIsDeferredLogFlush(boolean isFlush) {
		this.conf.setBoolean(HbaseConfiguration.OUTPUT_ISDEFERREDLOG_FLUSH, isFlush);
	}

	public boolean getOutputHBaseIsDeferredLogFlush() {
		return this.conf.getBoolean(HbaseConfiguration.OUTPUT_ISDEFERREDLOG_FLUSH, false);
	}

	/**
	 * 列的块大小（单位：字节）
	 * 
	 * @param size
	 */
	public void setOutputHBaseColmunBlocksize(int size) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_COLMUN_BLOCKSIZE, size);
	}

	public int getOutputHBaseColmunBlocksize() {
		// HFile block size默认是64KB
		return this.conf.getInt(HbaseConfiguration.OUTPUT_COLMUN_BLOCKSIZE, 64 * 1024);
	}

	/**
	 * 列的最大版本号
	 * 
	 * @param maxversion
	 */
	public void setOutputHBaseColmunMaxversion(int maxversion) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_COLMUN_MAXVERSION, maxversion);
	}

	public int getOutputHBaseColmunMaxversion() {
		// 默认为3
		return this.conf.getInt(HbaseConfiguration.OUTPUT_COLMUN_MAXVERSION, 3);
	}

	/**
	 * 列的最 小版本号
	 * 
	 * @param minversion
	 */
	public void setOutputHBaseColmunMinversion(int minversion) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_COLMUN_MINVERSION, minversion);
	}

	public int getOutputHBaseColmunMinversion() {
		// 默认为1
		return this.conf.getInt(HbaseConfiguration.OUTPUT_COLMUN_MINVERSION, 1);
	}

	/**
	 * bloomFilterType
	 * 
	 * @param type
	 */
	public void setOutputHBaseBloomFilterType(int type) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_BLOOMFILTERTYPE, type);
	}

	public BloomType getOutputHBaseBloomFilterType() {
		int type = this.conf.getInt(HbaseConfiguration.OUTPUT_BLOOMFILTERTYPE, BloomType_NONE);
		return this.getHBaseBloomTye(type);
	}

	/**
	 * 设置是否保存在内存中.
	 * 
	 * @param inMemory
	 */
	public void setOutputHBaseSetInMemory(boolean inMemory) {
		this.conf.setBoolean(HbaseConfiguration.OUTPUT_SET_INMEMORY, inMemory);
	}

	public Boolean getOutputHBaseSetInMemory() {
		return this.conf.getBoolean(HbaseConfiguration.OUTPUT_SET_INMEMORY, false);
	}

	/**
	 * 设置提交数据的缓存大小
	 * 
	 * @param commitBufferLength
	 */
	public void setOutputHBaseBufferLength(int commitBufferLength) {
		this.conf.setInt(HbaseConfiguration.OUTPUT_SET_COMMIT_BUFFERLENGTH, commitBufferLength);
	}

	public int getOutputHBaseBufferLength() {
		return this.conf.getInt(HbaseConfiguration.OUTPUT_SET_COMMIT_BUFFERLENGTH, 1000);
	}

	/**
	 * 获取删除数据标示符
	 * 
	 * @return
	 */
	public boolean getOutputHBaseDataDeleteFlag() {
		return conf.getBoolean(OUTPUT_DATA_DELETE_FLAG, false);
	}

	public void setOutputHBaseDataDeleteFlag(boolean flag) {
		conf.setBoolean(OUTPUT_DATA_DELETE_FLAG, flag);
	}

	/**
	 * 获取输出字段拆分符号
	 * 
	 * @return
	 */
	public String getOutputHBaseColumnSplitSign() {
		return conf.get(OUTPUT_HBASE_COLUMN_SPLIT_SIGN, null);
	}

	public void setOutputHBaseColumnSplitSign(String sign) {
		conf.set(OUTPUT_HBASE_COLUMN_SPLIT_SIGN, sign);
	}

	/**
	 * 获取输出字段拆分字段关系
	 * 
	 * @return
	 */
	public String getOutputHBaseColumnRelation() {
		return conf.get(OUTPUT_HBASE_COLUMN_RELATION, null);
	}

	public void setOutputHBaseColumnRelation(String flag) {
		conf.set(OUTPUT_HBASE_COLUMN_RELATION, flag);
	}

	/**
	 * 获取是否写WAL日志
	 * 
	 * @return
	 */
	public Durability getOutputHBaseSetWalFlag() {
		int flag = this.getOutputHBaseSetWalFlags();
		if (-1 == flag) {
			return null;
		}
		try {
			if (flag == 0)
				return Durability.USE_DEFAULT;
			else if (flag == 1)
				return Durability.SKIP_WAL;
			else if (flag == 2)
				return Durability.ASYNC_WAL;
			else if (flag == 3)
				return Durability.SYNC_WAL;
			else if (flag == 4)
				return Durability.FSYNC_WAL;
			else
				return Durability.USE_DEFAULT;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public int getOutputHBaseSetWalFlags() {
		return conf.getInt(OUTPUT_SET_WAL_FLAG, -1);
	}

	public void setOutputHBaseSetWalFlag(int flag) {
		conf.setInt(OUTPUT_SET_WAL_FLAG, flag);
	}

	public boolean getOutputHBaseUseOnlyRowkey() {
		return conf.getBoolean(OUTPUT_HBASE_USE_ONLYROWKEY, false);
	}
	
	public String getOutputHBaseJarName() {
		return conf.get(OUTPUT_HBASE_JAR_NAME, null);
	}

	public void setOutputHBaseJarName(String name) {
		conf.set(OUTPUT_HBASE_JAR_NAME, name);
	}

	/**
	 * 获取压缩或合并压缩的类型
	 * 
	 * @param type
	 *            压缩标识符
	 * @return 压缩类型 Algorithm
	 */
	private Algorithm getHBaseCompressionType(int type) {
		switch (type) {
		case COMPRESSION_ALGORITHM_NONE:
			return Algorithm.NONE;
		case COMPRESSION_ALGORITHM_LZO:
			return Algorithm.LZO;
		case COMPRESSION_ALGORITHM_GZ:
			return Algorithm.GZ;
		case COMPRESSION_ALGORITHM_SNAPPY:
			return Algorithm.SNAPPY;
		default:
			return Algorithm.NONE;
		}
	}

	/**
	 * 获取 StoreFile.BloomType
	 * 
	 * @param type
	 *            bloom标识
	 * @return StoreFile.BloomType
	 */
	private BloomType getHBaseBloomTye(int type) {
		switch (type) {
		case BloomType_NONE:
			return BloomType.NONE;
		case BloomType_ROW:
			return BloomType.ROW;
		case BloomType_ROWCOL:
			return BloomType.ROWCOL;
		default:
			return BloomType.NONE;
		}
	}
}