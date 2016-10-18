package com.ery.hadoop.mrddx;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;

import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBInputFormat;
import com.ery.hadoop.mrddx.db.DBOutputFormat;
import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.db.mapreduce.DataDrivenDBInputFormat;
import com.ery.hadoop.mrddx.db.mapreduce.OracleDataDrivenDBInputFormat;
import com.ery.hadoop.mrddx.db.mapreduce.PartitionDrivenDBInputFormat;
import com.ery.hadoop.mrddx.db.partitiondb.PartitionDBInputFormat;
import com.ery.hadoop.mrddx.db.partitiondb.PartitionDBOutputFormat;
import com.ery.hadoop.mrddx.db.rdbnorm.RDBInputFormat;
import com.ery.hadoop.mrddx.db.rdbnorm.RDBOutputFormat;
import com.ery.hadoop.mrddx.file.RCFileInputFormat;
import com.ery.hadoop.mrddx.file.RCFileOutputFormat;
import com.ery.hadoop.mrddx.file.SequenceFileInputFormat;
import com.ery.hadoop.mrddx.file.SequenceFileOutputFormat;
import com.ery.hadoop.mrddx.file.TextInputFormat;
import com.ery.hadoop.mrddx.file.TextOutputFormat;
import com.ery.hadoop.mrddx.hFile.HBaseHFileInputFormat;
import com.ery.hadoop.mrddx.hFile.HBaseHFileOutputFormat;
import com.ery.hadoop.mrddx.hbase.HbaseInputFormat;
import com.ery.hadoop.mrddx.hbase.HbaseOutputFormat;
import com.ery.hadoop.mrddx.hive.HiveInputFormat;
import com.ery.hadoop.mrddx.hive.HiveOutputFormat;
import com.ery.hadoop.mrddx.hive.HiveRCFileOutputFormat;
import com.ery.hadoop.mrddx.hive.HiveSequenceFileOutputFormat;
import com.ery.hadoop.mrddx.remote.FTPInputFormat;
import com.ery.hadoop.mrddx.remote.FTPOutputFormat;
import com.ery.hadoop.mrddx.vertica.VerticaInputFormat;
import com.ery.hadoop.mrddx.vertica.VerticaOutputFormat;

/**
 * 系统配置对象父类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-2
 * @version v1.0
 */
public class MRConfiguration implements Configurable {
	/**
	 * 原有系统参数名称
	 */
	public static final String MAPRED_JOB_NAME = MRJobConfig.JOB_NAME;// "mapred.job.name";
	public static final String MAPRED_JOB_PRIORITY = MRJobConfig.PRIORITY; // "mapred.job.priority";
	public static final String FS_DEFAULT_NAME = "fs.defaultFS";// "fs.default.name";
	public static final String MAPRED_JOB_TRACKER = "mapred.job.tracker.is.deprecated";// "mapreduce.jobtracker.address";
	// mapreduce.job.tracker mapreduce.jobtracker.address
	public static final String MAPRED_INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";// "mapred.input.dir";
	public static final String MAPRED_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir";// "mapred.output.dir";
	public static final String MAPRED_MAP_TASKS = MRJobConfig.NUM_MAPS;// "mapred.map.tasks";
	public static final String MAPRED_REDUCE_TASKS = MRJobConfig.NUM_REDUCES;// "mapred.reduce.tasks";
	public static final String MAPRED_JOB_QUEUE_NAME = MRJobConfig.QUEUE_NAME;// "mapred.job.queue.name";//
																				// mapreduce.job.queuename

	// 系统运行的fs名称
	public static final String MAPRED_FS_DEFAULT_NAME = "mr.mapred.sys.fs.default.name";
	// 系统运行的jobtracker名称
	public static final String MAPRED_JOB_TRACKER_NAME = "mr.mapred.sys.mapred.job.tracker";

	/**
	 * 与业务无关的参数（INTERNAL_开头的参数，属于内部逻辑定义的，无需在客户端配置）
	 */
	public static final String SYS_HADOOP_HOME = "sys.hadoop.home";
	public static final String SYS_HBASE_HOME = "sys.hbase.home";
	public static final String SYS_JOB_ID = "mr.sys.job.id";
	public static final String INTERNAL_SYS_RUN_JOB_CMD = "internal.mr.sys.run.job.cmd"; // 运行job的命令
	public static final String INTERNAL_SYS_ISNEED_ZK_MONITOR = "internal.mr.sys.isneed.zk.monitor";// 是否需要zk监听.true:表示监听，否则不监听
	public static final String SYS_ZK_ADDRESS = "sys.zk.job.address"; // zk的连接地址
	public static final String SYS_ZK_ROOT_PATH = "sys.zk.job.root.path"; // job下zk的根路径
	public static final String SYS_ONEFILE_ONEMAP = "sys.onefile.onemap"; // 一个文件分为一个map

	public static final String SYS_MAP_RAOUTE_TYPE = "sys.map.raoute.type"; // map主机选择规则
																			// host
																			// map
																			// default
																			// none

	public static final String SYS_JOB_OUTPUT_ROOT_PATH = "sys.mapered.job.output.root.dir"; // job输出目录的根路径
	public static final String INTERNAL_SYS_ZK_JOB_MAPPER_PATH = "internal.mr.sys.zk.job.mapper.path";// mapper节点路径
	public static final String INTERNAL_SYS_ZK_JOB_REDUCE_PATH = "internal.mr.sys.zk.job.reduce.path";// reduce节点路径
	public static final String INTERNAL_SYS_ZK_JOB_FTP_PATH = "internal.mr.sys.zk.job.ftp.path";// ftp节点路径
	public static final String INTERNAL_SYS_ZK_JOB_JOBID_PATH = "internal.mr.sys.zk.job.jobid.path";// JOBID节点路径
	public static final String INTERNAL_SYS_ZK_FILE_LOCK_FLAG = "sys.zk.file.lock.flag";// 是否监听处理文件的标示符，true：表示监听，默认为true

	// 运行 文件下载和处理的标示符（0:下载、1：下载并处理、2：处理）
	public static final String INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE = "internal.sys.file.down.or.do.ftptype";
	public static final String INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN = "0";
	public static final String INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO = "1";
	public static final String INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DO = "2";

	// 上传文件标示符(-1:表示不上传，0:上传)
	public static final String INTERNAL_SYS_FILE_UP_FTPTYPE = "internal.sys.file.up.ftptype";
	public static final String INTERNAL_SYS_FILE_UP_FTPTYPE_UP = "0";

	/**
	 * 是否是测试JOB标示符，true表示测试JOB，否则是运行JOB
	 */
	public static final String TEST_JOB_FLAG = "TEST_JOB_FLAG";
	/**
	 * 日志参数
	 */
	// 日志的连接参数
	public static final String JOB_SYS_LOG_JDBC_URL = "sys.log.conf.jdbc.url";
	public static final String JOB_SYS_LOG_JDBC_USER = "sys.log.conf.jdbc.username";
	public static final String JOB_SYS_LOG_JDBC_PWD = "sys.log.conf.jdbc.password";

	// 日志的内部参数
	public static final String INTERNAL_JOB_LOG_ID = "internal.mr.sys.log.log.id";
	public static final String INTERNAL_JOB_LOG_ROWRECORD = "internal.mr.sys.log.job.rowrecord";
	// map和reduce实时记录数据量
	public static final String INTERNAL_JOB_LOG_MAP_INPUT_PER_RECORD_NUMBER = "sys.log.conf.map.input.per.record.number";
	public static final String INTERNAL_JOB_LOG_MAP_OUTPUT_PER_RECORD_NUMBER = "sys.log.conf.map.output.per.record.number";
	public static final String INTERNAL_JOB_LOG_REDUCE_INPUT_PER_RECORD_NUMBER = "sys.log.conf.reduce.input.per.record.number";
	public static final String INTERNAL_JOB_LOG_REDUCE_OUTPUT_PER_RECORD_NUMBER = "sys.log.conf.reduce.output.per.record.number";

	// 调试日志参数
	public static final String INTERNAL_JOB_LOG_DEBUG = "internal.mr.sys.log.log.debug";// 日志级别
	public static final int INTERNAL_JOB_LOG_DEBUG_CONF_INFO = 1; // 日志级别,
	// 打印map和reduce的原始配置信息
	public static final int INTERNAL_JOB_LOG_DEBUG_DECONF_INFO = 2; // 　日志级别,
	// 打印map和reduce的解析后的日志信息(包括级别1)
	public static final int INTERNAL_JOB_LOG_DEBUG_RECORD = 3; // 　日志级别,
	// 打印map和reduce的输入输出记录数日志信息(包括级别2)
	public static final String INTERNAL_JOB_LOG_DEBUG_ROWNUM = "internal.mr.sys.log.log.debug.rownum"; // 打印记录的条数
	// 数据采集时，是否打印日志到控制台
	public static final String INTERNAL_LOG_CONF_COL_IS_PRINTCONSOLE = "sys.log.conf.col.is.print.msg.console";
	/**
	 * 新增业务系统参数名称
	 */
	// 系统文件配置
	public static final String SYS_INPUT_FORMAT_TYPE = "input.sys.mr.mapred.type";
	public static final String SYS_OUTPUT_FORMAT_TYPE = "output.sys.mr.mapred.type";

	public static final String SYS_RESOURCE = "input.sys.mr.mapred.resource";
	public static final String SYS_MAP_IS_COMBINER = "input.sys.mr.mapred.maper.is.combiner";
	// 输入类型Class name implementing DBWritable
	public static final String SYS_INPUT_CLASS_PROPERTY = "input.sys.mr.mapred.class";
	// 输入数据只过滤，不入库
	public static final String SYS_INPUT_ONLY_FILTER_NOT_STORAGE = "input.sys.mr.mapred.only.filter.not.storage";
	// 数据过滤掉的数据不写文件，true:过滤掉的数据不写文件,默认为true
	public static final String SYS_INPUT_FILTER_ILLEGALDATA_WRITEFILES = "input.sys.mr.mapred.filter.illegal.data.write.files";
	// 标识map是否直接输出
	public static final String SYS_INPUT_IS_MAP_END = "input.sys.mr.mapred.is.map.end";
	// 输入字段
	public static final String SYS_INPUT_FIELD_NAMES_PROPERTY = "input.sys.mr.mapred.field.names";
	// 输出字段
	public static final String SYS_OUTPUT_FIELD_NAMES_PROPERTY = "output.sys.mr.mapred.field.names";
	// 分组字段关系,格式（目标字段:源字段:源字段的统计方法）例如： SERV_ID:SERV_ID:NONE,CHARGE:CHARGE:SUM
	public static final String SYS_RELATED_GROUP_BY_FIELD_METHOD_REL = "output.sys.mr.mapred.group.field.method.rel";
	// move文件的目录(无需配置)
	public static final String SYS_OUTPUT_FILE_TARGET_PATH = "output.sys.mr.mapred.file.target.Path";
	// 输入高级应用配置项(operation1:(字段1*2+字段3-2)=字段4,operation1:....)
	public static final String SYS_INPUT_MAP_SENIOR_CONF = "input.sys.mr.mapred.map.senior.conf";
	// 输出高级应用配置项(operation1:(字段1*2+字段3-2)=字段1,operation1:....)
	public static final String SYS_OUTPUT_REDUCE_SENIOR_CONF = "output.sys.mr.mapred.reduce.senior.conf";

	// 输入的过滤高级应用中，是否输出过滤的内容
	public static final String SYS_INPUT_MAP_SENIOR_FILTER_IS_OUTPUT = "input.sys.mr.mapred.map.senior.filter.isoutput";
	// 输入的过滤高级应用中，输出过滤的内容的目录
	public static final String SYS_INPUT_MAP_SENIOR_FILTER_OUTPUT_PATH = "input.sys.mr.mapred.map.senior.filter.output.path";
	// 输入高级应用中，operation filter针对输入数据的过滤(默认为true, true表示匹配正则表达式的数据过滤掉,
	// false表示匹配正则表达式的数据不过滤掉)
	public static final String SYS_INPUT_FILTER_FLAG = "input.sys.mr.mapred.map.filter.flag";
	// 输入高级应用中，operation filter针对输出数据的过滤(默认为true, true表示匹配正则表达式的数据过滤掉,
	// false表示匹配正则表达式的数据不过滤掉)
	public static final String SYS_OUTPUT_FILTER_FLAG = "output.sys.mr.mapred.map.filter.flag";

	// 输入数据源ID
	public static final String SYS_INPUT_DATA_SOURCE_ID = "input.sys.mr.mapred.data.source.id";
	// 输出数据源ID
	public static final String SYS_OUTPUT_DATA_SOURCE_ID = "output.sys.mr.mapred.data.source.id";

	// 输出字段的默认值,例如：[a:defvaluea],[b:defvalueb]
	public static final String SYS_OUTPUT_COLUMN_DEFAULT_VALUE = "output.sys.mr.maperd.outcolumn.default.value";

	// 闪断重连等待时间
	public static final String CONNECT_TIME = "connect.timeinterval";
	// 闪断重连最大次数
	public static final String CONNECT_MAX = "connect.trytimes";
	public static final String CONNECT_TIMEOUT = "connect.timeout";
	public static final String FTP_CONNECT_MODE = "ftp.connect.mode";
	public static final String FTP_CONNECT_ENCODE = "ftp.connect.encode";
	public static final String FTP_CONNECT_TRANSTYPE = "ftp.connect.transtype.isbinnary";

	public static final String FILE_CONTENT_ENCODING = "input.file.content.encoding";// 文件源编码
	public static final String FILE_CONTENT_ENCODING_DEFAULT = "utf-8";

	// Hadoop配置对象
	protected Configuration conf;

	/**
	 * 插件参数
	 */
	// map端的插件类名(除包名之外)
	public static final String SYS_INPUT_MAP_PLUGIN_CLASSNAME = "input.sys.mr.mapred.map.plugin.classname";

	// map端的插件代码
	public static final String SYS_INPUT_MAP_PLUGIN_CODE = "input.sys.mr.mapred.map.plugin.code";

	// reduce端的插件类名(除包名之外)
	public static final String SYS_OUTPUT_REDUCE_PLUGIN_CLASSNAME = "output.sys.mr.mapred.reduce.plugin.classname";

	// reduce端的插件代码
	public static final String SYS_OUTPUT_REDUCE_PLUGIN_CODE = "output.sys.mr.mapred.reduce.plugin.code";

	/**
	 * 构造方法
	 * 
	 * @param job
	 */
	public MRConfiguration(Configuration job) {
		this.conf = job;
	}

	public Configuration getConf() {
		return conf;
	}

	// 与业务无关的参数
	public void setSysHadoopHome(String home) {
		conf.set(SYS_HADOOP_HOME, home);
	}

	public String getSysHadoopHome() {
		return conf.get(SYS_HADOOP_HOME, null);
	}

	public void setConnectMax(String connectMax) {
		conf.set(CONNECT_MAX, connectMax);
	}

	public String getConnectMax() {
		return conf.get(CONNECT_MAX, null);
	}

	public void setConnectTime(String connectTime) {
		conf.set(CONNECT_TIME, connectTime);
	}

	public String getConnectTime() {
		return conf.get(CONNECT_TIME, null);
	}

	public void setSysHBaseHome(String home) {
		conf.set(SYS_HBASE_HOME, home);
	}

	public String getSysHBaseHome() {
		return conf.get(SYS_HBASE_HOME, null);
	}

	public void setSysJobId(long id) {
		conf.setLong(SYS_JOB_ID, id);
	}

	public long getSysJobId() {
		return conf.getLong(SYS_JOB_ID, 0);
	}

	public String getRunJobCmd() {
		return conf.get(INTERNAL_SYS_RUN_JOB_CMD, "");
	}

	public void setZKMonitor(boolean monitor) {
		conf.setBoolean(INTERNAL_SYS_ISNEED_ZK_MONITOR, monitor);
	}

	public boolean isZKMonitor() {
		return conf.getBoolean(INTERNAL_SYS_ISNEED_ZK_MONITOR, false);
	}

	public void setZKAddress(String address) {
		conf.set(SYS_ZK_ADDRESS, address);
	}

	public String getZKAddress() {
		return conf.get(SYS_ZK_ADDRESS, null);
	}

	public void setZKRootPath(String rootPath) {
		conf.set(SYS_ZK_ROOT_PATH, rootPath);
	}

	public String getZKRootPath() {
		return conf.get(SYS_ZK_ROOT_PATH, "/mrddx");
	}

	public boolean getOneFileOneMap() {
		return conf.getBoolean(SYS_ONEFILE_ONEMAP, false);
	}

	public String getJobOutputRootPath() {
		return conf.get(SYS_JOB_OUTPUT_ROOT_PATH, "/mrddx/output");
	}

	public void setZKMapperPath(String mapperNodePath) {
		conf.set(INTERNAL_SYS_ZK_JOB_MAPPER_PATH, mapperNodePath);

	}

	public String getZKMapperPath() {
		return conf.get(INTERNAL_SYS_ZK_JOB_MAPPER_PATH, null);
	}

	public void setZKReducePath(String mapperNodePath) {
		conf.set(INTERNAL_SYS_ZK_JOB_REDUCE_PATH, mapperNodePath);
	}

	public String getZKReducePath() {
		return conf.get(INTERNAL_SYS_ZK_JOB_REDUCE_PATH, null);
	}

	public void setZKFTPPath(String ftpNodePath) {
		conf.set(INTERNAL_SYS_ZK_JOB_FTP_PATH, ftpNodePath);
	}

	public String getZKFTPPath() {
		return conf.get(INTERNAL_SYS_ZK_JOB_FTP_PATH, null);
	}

	public void setZKJOBIdPath(String jobidNodePath) {
		conf.set(INTERNAL_SYS_ZK_JOB_JOBID_PATH, jobidNodePath);
	}

	public String getZKJOBIdPath() {
		return conf.get(INTERNAL_SYS_ZK_JOB_JOBID_PATH, null);
	}

	public boolean getZKFileLockFlag() {
		return conf.getBoolean(INTERNAL_SYS_ZK_FILE_LOCK_FLAG, true);
	}

	// 日志参数
	public String getLogJDBCUrl() {
		return conf.get(JOB_SYS_LOG_JDBC_URL, null);
	}

	public String getLogJDBCUser() {
		return conf.get(JOB_SYS_LOG_JDBC_USER, null);
	}

	public String getLogJDBCPwd() {
		return conf.get(JOB_SYS_LOG_JDBC_PWD, null);
	}

	public void setJobLogId(long logId) {
		conf.setLong(INTERNAL_JOB_LOG_ID, logId);
	}

	public long getJobLogId() {
		return conf.getLong(INTERNAL_JOB_LOG_ID, -1);
	}

	public void setJOBLogRowcord(long count) {
		long temp = conf.getLong(INTERNAL_JOB_LOG_ROWRECORD, 0);
		conf.setLong(INTERNAL_JOB_LOG_ROWRECORD, temp + count);
	}

	public long getJOBLogRowcord() {
		return conf.getLong(INTERNAL_JOB_LOG_ROWRECORD, 0);
	}

	public void SetJOBLogMapInputPerRecordNumber(long number) {
		conf.setLong(INTERNAL_JOB_LOG_MAP_INPUT_PER_RECORD_NUMBER, number);
	}

	public long getJOBLogMapInputPerRecordNumber() {
		return conf.getLong(INTERNAL_JOB_LOG_MAP_INPUT_PER_RECORD_NUMBER, 10000);
	}

	public void setJOBLogMapOutputPerRecordNumber(long number) {
		conf.getLong(INTERNAL_JOB_LOG_MAP_OUTPUT_PER_RECORD_NUMBER, number);
	}

	public long getJOBLogMapOutputPerRecordNumber() {
		return conf.getLong(INTERNAL_JOB_LOG_MAP_OUTPUT_PER_RECORD_NUMBER, 10000);
	}

	public void setJOBLogReduceInputPerRecordNumber(long number) {
		conf.setLong(INTERNAL_JOB_LOG_REDUCE_INPUT_PER_RECORD_NUMBER, number);
	}

	public long getJOBLogReduceInputPerRecordNumber() {
		return conf.getLong(INTERNAL_JOB_LOG_REDUCE_INPUT_PER_RECORD_NUMBER, 10000);
	}

	public void setJOBLogReduceOutputPerRecordNumber(long number) {
		conf.setLong(INTERNAL_JOB_LOG_REDUCE_OUTPUT_PER_RECORD_NUMBER, number);
	}

	public long getJOBLogReduceOutputPerRecordNumber() {
		return conf.getLong(INTERNAL_JOB_LOG_REDUCE_OUTPUT_PER_RECORD_NUMBER, 10000);
	}

	public int getJobLogDebug() {
		return conf.getInt(MRConfiguration.INTERNAL_JOB_LOG_DEBUG, -1);
	}

	public int getJobLogDebugRowNum() {
		return conf.getInt(MRConfiguration.INTERNAL_JOB_LOG_DEBUG_ROWNUM, -1);
	}

	public boolean getLogConfColIsPrintConsole() {
		return conf.getBoolean(MRConfiguration.INTERNAL_LOG_CONF_COL_IS_PRINTCONSOLE, false);
	}

	// 原有系统配置
	public String getJobName() {
		return conf.get(MAPRED_JOB_NAME);
	}

	public JobPriority decodeTempMapredJobPriority() {
		int flag = conf.getInt(MAPRED_JOB_PRIORITY, 3);
		JobPriority tempJobPriority = null;
		switch (flag) {
		case 1:
			tempJobPriority = JobPriority.VERY_LOW;
			break;
		case 2:
			tempJobPriority = JobPriority.LOW;
			break;
		case 3:
			tempJobPriority = JobPriority.NORMAL;
			break;
		case 4:
			tempJobPriority = JobPriority.HIGH;
			break;
		case 5:
			tempJobPriority = JobPriority.VERY_HIGH;
			break;
		default:
			break;
		}

		if (null == tempJobPriority) {
			return null;
		}

		conf.set(MAPRED_JOB_PRIORITY, tempJobPriority.toString());
		return tempJobPriority;
	}

	public JobPriority getTempMapredJobPriority() {
		return JobPriority.valueOf(conf.get(MAPRED_JOB_PRIORITY, JobPriority.NORMAL.toString()));
	}

	public String getFSDefaultName() {
		return conf.get(FS_DEFAULT_NAME);
	}

	public String getMapredJobTracker() {
		return conf.get(MAPRED_JOB_TRACKER);
	}

	public String getMapredInputDir() {
		return conf.get(MAPRED_INPUT_DIR);
	}

	public void setMapredOutputDir(String path) {
		conf.set(MAPRED_OUTPUT_DIR, path);
	}

	public String getMapredOutputDir() {
		return conf.get(MAPRED_OUTPUT_DIR);
	}

	public int getNumMapTasks() {
		return conf.getInt(MAPRED_MAP_TASKS, 1);
	}

	public void setNumMapTasks(int num) {
		conf.setInt(MAPRED_MAP_TASKS, num);
	}

	public int getNumReduceTasks() {
		return conf.getInt(MAPRED_REDUCE_TASKS, 1);
	}

	public void setNumReduceTasks(int num) {
		conf.setInt(MAPRED_REDUCE_TASKS, num);
	}

	public String getMapredJobQueueName() {
		return conf.get(MAPRED_JOB_QUEUE_NAME, "");
	}

	public void setMapredJobQueueName(String queueName) {
		conf.set(MAPRED_JOB_QUEUE_NAME, queueName);
	}

	// 新增业务的系统参数
	public void setResource(String resource) {
		conf.set(SYS_RESOURCE, resource);
	}

	public String[] getResource() {
		return conf.getStrings(SYS_RESOURCE, new String[0]);
	}

	/**
	 * 是否map直接输出结果，true:直接输出，默认为false 注：若为true，不能设置该变量conf.setCombinerClass；
	 * 设置reduce的任务数conf.setNumReduceTasks为0；
	 * 
	 * @param flag
	 */
	public void setInputMapEnd(boolean flag) {
		conf.setBoolean(SYS_INPUT_IS_MAP_END, flag);
	}

	public boolean getInputMapEnd() {
		return conf.getBoolean(SYS_INPUT_IS_MAP_END, false);
	}

	public void setInputMapOnlyFilterNotStorage(boolean flag) {
		conf.setBoolean(SYS_INPUT_ONLY_FILTER_NOT_STORAGE, flag);
	}

	public boolean getInputMapOnlyFilterNotStorage() {
		return conf.getBoolean(SYS_INPUT_ONLY_FILTER_NOT_STORAGE, false);
	}

	public boolean getInputMapFilterIllegalDatawriteFiles() {
		return conf.getBoolean(SYS_INPUT_FILTER_ILLEGALDATA_WRITEFILES, true);
	}

	public void setInputIsCombiner(boolean flag) {
		conf.setBoolean(SYS_MAP_IS_COMBINER, flag);
	}

	public boolean getInputIsCombiner() {
		return conf.getBoolean(SYS_MAP_IS_COMBINER, false);
	}

	public Class<?> getInputClass() {
		return conf.getClass(DBConfiguration.SYS_INPUT_CLASS_PROPERTY, NullDBWritable.class);
	}

	public void setInputClass(Class<?> inputClass) {
		conf.setClass(DBConfiguration.SYS_INPUT_CLASS_PROPERTY, inputClass, DBWritable.class);
	}

	public String getInputFormatClassName(String type) {
		if (DataTypeConstant.FILETEXT.equalsIgnoreCase(type)) {
			return TextInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILESEQUENCE.equalsIgnoreCase(type)) {
			return SequenceFileInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILERCFILE.equalsIgnoreCase(type)) {
			return RCFileInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVETEXT.equalsIgnoreCase(type)) {
			return HiveInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVESEQUENCE.equalsIgnoreCase(type)) {
			return HiveInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVERCFILE.equalsIgnoreCase(type)) {
			return HiveInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HBASE.equalsIgnoreCase(type)) {
			return HbaseInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.RDBNORM.equalsIgnoreCase(type)) {
			return RDBInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.MYSQLROW.equalsIgnoreCase(type)) {
			return DBInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.MYSQLDATA.equalsIgnoreCase(type)) {
			return DataDrivenDBInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.ORACLEROW.equalsIgnoreCase(type)) {
			return DBInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.ORACLEDATA.equalsIgnoreCase(type)) {
			return OracleDataDrivenDBInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.ORACLEPART.equalsIgnoreCase(type)) {
			return PartitionDrivenDBInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FTPDATA.equalsIgnoreCase(type)) {
			return FTPInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.MRHFILE.equalsIgnoreCase(type)) {
			return HBaseHFileInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.VERTICA.equalsIgnoreCase(type)) {
			return VerticaInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.POSTGRESQL.equalsIgnoreCase(type)) {
			return DBInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.PARTITION_SPLIT.equalsIgnoreCase(type)) {
			return PartitionDBInputFormat.class.getCanonicalName();
		}

		return null;
	}

	public String getOutputFormatClassName(String type) {
		if (DataTypeConstant.FILETEXT.equalsIgnoreCase(type)) {
			return TextOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILESEQUENCE.equalsIgnoreCase(type)) {
			return SequenceFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILERCFILE.equalsIgnoreCase(type)) {
			return RCFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVETEXT.equalsIgnoreCase(type)) {
			return HiveOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVESEQUENCE.equalsIgnoreCase(type)) {
			return HiveSequenceFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVERCFILE.equalsIgnoreCase(type)) {
			return HiveRCFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HBASE.equalsIgnoreCase(type)) {
			return HbaseOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.RDBNORM.equalsIgnoreCase(type)) {
			return RDBOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.MYSQLROW.equalsIgnoreCase(type)) {
			return DBOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.MYSQLDATA.equalsIgnoreCase(type)) {
			return DBOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.ORACLEROW.equalsIgnoreCase(type)) {
			return DBOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.ORACLEDATA.equalsIgnoreCase(type)) {
			return DBOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.ORACLEPART.equalsIgnoreCase(type)) {
			return DBOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FTPDATA.equalsIgnoreCase(type)) {
			return FTPOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.MRHFILE.equalsIgnoreCase(type)) {
			return HBaseHFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.MRHBASE.equalsIgnoreCase(type)) {
			return HBaseHFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.VERTICA.equalsIgnoreCase(type)) {
			return VerticaOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.POSTGRESQL.equalsIgnoreCase(type)) {
			return DBOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.PARTITION_SPLIT.equalsIgnoreCase(type)) {
			return PartitionDBOutputFormat.class.getCanonicalName();
		}
		return null;
	}

	public String getDownLoadDataInputFormatClassName(String type) {
		if (DataTypeConstant.FILETEXT.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.file.TextInputFormat";
			return TextInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILESEQUENCE.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.file.SequenceFileInputFormat";
			return SequenceFileInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILERCFILE.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.file.RCFileInputFormat";
			return RCFileInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVETEXT.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.hive.HiveInputFormat";
			return HiveInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVESEQUENCE.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.hive.HiveInputFormat";
			return HiveInputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVERCFILE.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.hive.HiveInputFormat";
			return HiveInputFormat.class.getCanonicalName();
		}

		return null;
	}

	public String getUpLoadDataOutputFormatClassName(String type) {
		if (DataTypeConstant.FILETEXT.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.file.TextOutputFormat";
			return TextOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILESEQUENCE.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.file.SequenceFileOutputFormat";
			return SequenceFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.FILERCFILE.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.file.RCFileOutputFormat";
			return RCFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVETEXT.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.hive.HiveOutputFormat";
			return HiveOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVESEQUENCE.equalsIgnoreCase(type)) {
			// return
			// "com.ery.hadoop.mrddx.hive.HiveSequenceFileOutputFormat";
			return HiveSequenceFileOutputFormat.class.getCanonicalName();
		} else if (DataTypeConstant.HIVERCFILE.equalsIgnoreCase(type)) {
			// return "com.ery.hadoop.mrddx.hive.HiveRCFileOutputFormat";
			return HiveRCFileOutputFormat.class.getCanonicalName();
		}
		return null;
	}

	public List<Class<?>> getInputFormatClass() throws ClassNotFoundException {
		String types[] = conf.getStrings(SYS_INPUT_FORMAT_TYPE, new String[0]);
		if (types.length <= 0) {
			return null;
		}

		List<Class<?>> lst = new ArrayList<Class<?>>();
		for (String t : types) {
			String className = getInputFormatClassName(t);
			if (null == className || className.trim().length() <= 0) {
				return null;
			}
			lst.add(Class.forName(className));
		}

		return lst;
	}

	public List<Class<?>> getOutputFormatClass() throws ClassNotFoundException {
		String types[] = conf.getStrings(SYS_OUTPUT_FORMAT_TYPE, new String[0]);
		if (types.length <= 0) {
			return null;
		}

		List<Class<?>> lst = new ArrayList<Class<?>>();
		for (String t : types) {
			String className = getOutputFormatClassName(t);
			if (null == className || className.trim().length() <= 0) {
				return null;
			}
			lst.add(Class.forName(className));
		}

		return lst;
	}

	public String[] getInputFormatType() {
		return conf.getStrings(SYS_INPUT_FORMAT_TYPE, new String[0]);
	}

	public String[] getOutputFormatType() {
		return conf.getStrings(SYS_OUTPUT_FORMAT_TYPE, new String[0]);
	}

	public String[] getInputFieldNames() {
		String param[] = conf.getStrings(DBConfiguration.SYS_INPUT_FIELD_NAMES_PROPERTY);
		if (null == param) {
			return null;
		}
		return param;
	}

	public void setInputFieldNames(String fieldNames) {
		conf.set(DBConfiguration.SYS_INPUT_FIELD_NAMES_PROPERTY, fieldNames);
	}

	public String[] getOutputFieldNames() {
		String param[] = conf.getStrings(DBConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY);
		if (null == param) {
			return null;
		}
		return param;
	}

	public void setOutputFieldNames(String[] fieldNames) {
		for (int i = 0; i < fieldNames.length; i++) {
			fieldNames[i] = fieldNames[i].toLowerCase();
		}
		conf.setStrings(DBConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY, fieldNames);
	}

	public String[] getRelatedGroupFieldMethod() {
		String[] param = conf.getStrings(SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
		if (null == param) {
			return null;
		}
		return param;
	}

	/**
	 * 获取分组字段关系
	 * 
	 * @param method
	 */
	public void setRelatedGroupFieldMethod(String method) {
		conf.setStrings(SYS_RELATED_GROUP_BY_FIELD_METHOD_REL, method);
	}

	public String getOutputTargetPath() {
		return conf.get(SYS_OUTPUT_FILE_TARGET_PATH, "");
	}

	public void setOutputTargetPath(String path) {
		conf.set(SYS_OUTPUT_FILE_TARGET_PATH, path);
	}

	public String getInputMapSeniorConf() {
		return conf.get(SYS_INPUT_MAP_SENIOR_CONF, null);
	}

	public void setInputMapSeniorConf(String path) {
		conf.set(SYS_INPUT_MAP_SENIOR_CONF, path);
	}

	public String getOutputReduceSeniorConf() {
		return conf.get(SYS_OUTPUT_REDUCE_SENIOR_CONF, null);
	}

	public void setOutputReduceSeniorConf(String path) {
		conf.set(SYS_OUTPUT_REDUCE_SENIOR_CONF, path);
	}

	public boolean isOutputMapSeniorFilterIsOutput() {
		return conf.getBoolean(SYS_INPUT_MAP_SENIOR_FILTER_IS_OUTPUT, false);
	}

	public String getOutputMapSeniorFilterOutputPath() {
		return conf.get(SYS_INPUT_MAP_SENIOR_FILTER_OUTPUT_PATH, null);
	}

	public boolean isInputMapSeniorFilterFlag() {
		return conf.getBoolean(SYS_INPUT_FILTER_FLAG, true);
	}

	public boolean isOutputMapSeniorFilterFlag() {
		return conf.getBoolean(SYS_OUTPUT_FILTER_FLAG, true);
	}

	public boolean isTestJob() {
		return conf.getBoolean(TEST_JOB_FLAG, false);
	}

	public int getFileDownOrDoFTPType() {
		return conf.getInt(INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE, -1);
	}

	public int getFileUPFTPType() {
		return conf.getInt(INTERNAL_SYS_FILE_UP_FTPTYPE, -1);
	}

	public int getInputDataSourceId() {
		return conf.getInt(SYS_INPUT_DATA_SOURCE_ID, -1);
	}

	public int getOutputDataSourceId() {
		return conf.getInt(SYS_OUTPUT_DATA_SOURCE_ID, -1);
	}

	/**
	 * 获取直接输出的输出字段默认值
	 * 
	 * @return
	 */
	public String[] getOutputColumnDefaultValue() {
		return conf.getStrings(SYS_OUTPUT_COLUMN_DEFAULT_VALUE, new String[0]);
	}

	public void setOutputColumnDefaultValue(String value) {
		conf.set(SYS_OUTPUT_COLUMN_DEFAULT_VALUE, value);
	}

	/**
	 * 获取map端的插件类名
	 * 
	 * @return
	 */
	public String getInputMapPluginClassName() {
		return conf.get(SYS_INPUT_MAP_PLUGIN_CLASSNAME, null);
	}

	public void setInputMapPluginClassName(String value) {
		conf.set(SYS_INPUT_MAP_PLUGIN_CLASSNAME, value);
	}

	/**
	 * 获取map端的插件代码
	 * 
	 * @return
	 */
	public String getInputMapPluginCode() {
		return conf.get(SYS_INPUT_MAP_PLUGIN_CODE, null);
	}

	public void setInputMapPluginCode(String value) {
		conf.set(SYS_INPUT_MAP_PLUGIN_CODE, value);
	}

	/**
	 * 获取reduce端的插件类名
	 * 
	 * @return
	 */
	public String getOutputMapPluginClassName() {
		return conf.get(SYS_OUTPUT_REDUCE_PLUGIN_CLASSNAME, null);
	}

	public void setOutputMapPluginClassName(String value) {
		conf.set(SYS_OUTPUT_REDUCE_PLUGIN_CLASSNAME, value);
	}

	/**
	 * 获取reduce端的插件代码
	 * 
	 * @return
	 */
	public String getOutputMapPluginCode() {
		return conf.get(SYS_OUTPUT_REDUCE_PLUGIN_CODE, null);
	}

	public void setOutputMapPluginCode(String value) {
		conf.set(SYS_OUTPUT_REDUCE_PLUGIN_CODE, value);
	}

	public CompressionType getOutputCompressionType() {
		return getOutputCompressionType(conf);
	}

	public static CompressionType getOutputCompressionType(Configuration conf) {
		String comType = conf.get("mapred.output.compression.type", null);
		if (comType != null && !comType.equals("")) {
			return CompressionType.valueOf(comType);
		} else {
			comType = conf.get("mapreduce.output.fileoutputformat.compress.type", CompressionType.RECORD.toString());
			return CompressionType.valueOf(comType);
		}
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public String getJobID() {
		return getConf().get(JobContext.ID);
	}

	public String getTaskID() {// "mapred.tip.id"
		return getConf().get(JobContext.TASK_ID);
	}
}
