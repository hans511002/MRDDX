package com.ery.hadoop.mrddx;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import com.ery.hadoop.mrddx.before.FileMapperBefore;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.FileSplit;
import com.ery.hadoop.mrddx.file.RCFileParse;
import com.ery.hadoop.mrddx.hive.HiveConfiguration;
import com.ery.hadoop.mrddx.log.MRJobMapDataLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.remote.plugin.IRemotePlugin;
import com.ery.hadoop.mrddx.senior.IMRSeniorApply;
import com.ery.hadoop.mrddx.senior.MRSeniorUtil;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.zk.IMonitorZKNode;
import com.ery.hadoop.mrddx.zk.MRWatcher;
import com.ery.hadoop.mrddx.zk.MonitorZKNode;

/**
 * 处理文本的Mapper抽象类 Copyrights @ 2012,Tianyuan DIC Information Co.,Ltd. All rights
 * reserved.
 * 
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 */
public class RCFileMapper extends Mapper<Object, BytesRefArrayWritable, DBRecord, DBRecord> implements IDBMonitor,
		IDBSenior {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(RCFileMapper.class);
	public static final MRLog MRLOG = MRLog.getInstance();
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	public int debug = -1; // 日志级别 1：打印job配置参数,
	// 2:打印job解析后的参数(包含1)，3：打印map和redue的输入输出记录
	public int debugRowNum = -1; // 每个map输入条数限制(设置-debug参数时，才生效, 默认值：1000)

	// map的拆分对象
	private InputSplit inputSplit;

	/**
	 * 以下为日志参数
	 */
	// map执行是否成功, 取值范围[1-2]:1-成功,2-失败
	private int runflag = 1;

	// job的日志id号
	private long jobLogId;

	// 当前map是否实际运行过(true-运行过,false-未运行过)
	private boolean isRun;

	// 输出的记录数
	private long outCount;

	// 无效的记录数
	private long recordInvalidCount;

	// 实时记录输入数据量
	private long perInputNumber;

	// 实时记录输入数据量
	private long perOutputNumber;

	// 记录数据的日志id
	private long fileDataLogId;

	// 过滤数据存放的根路径
	private String filterFileRootPath;

	// 过滤数据的输出流
	private FSDataOutputStream filterFSOStream;

	// 文件路径
	private String filePath;

	// 文件的过滤数据存放路径
	private String fileterFilePath = "";

	/**
	 * 以下为业务参数
	 */
	// 监听节点对象
	private IMonitorZKNode monitorZKNode;

	// 是否需要监控
	private boolean isNeedZKMonitor;

	/**
	 * 执行MAP前的预处理参数
	 */
	FileMapperBefore mrBefore;

	/**
	 * 执行任务的常规配置
	 */
	// 配置信息
	private FileConfiguration dbconf;

	// jobID
	private long jobId;

	// 系统运行的jobID
	private String sysJobId;

	// 任务ID
	private String taskId;

	// 读取到的记录数
	long recordCount;

	// 是否map直接输出结果（true：直接输出）
	private boolean inputMapEnd;

	// 输入字段名称
	private String[] srcFieldNames;

	// 拆分列的分隔符
	@SuppressWarnings("unused")
	private String fieldSplitChars;

	// 拆分行记录分隔符
	@SuppressWarnings("unused")
	private String rowSplitChars;

	// 目标字段名称
	private String[] destFieldNames;

	// 源字段名称
	private String[] destRelSrcFieldNames;

	// rc解析类
	private RCFileParse rcfileParse;

	// 字段方法名称
	private String[] groupFieldMethod;

	// 高级应用对象
	private IMRSeniorApply mrSeniorApply[];

	// 每个文件开头跳过的行数
	private int perFileSkipRowNum = -1;

	// 存放文件开头跳过的行数
	private int skipRowNum = -1;

	// 数据只过滤，不入库标示符，true:真
	private boolean onlyFilterNotStorage;

	// 数据过滤掉的数据写文件，true:过滤掉的数据写文件
	private boolean filterIllegalDataWriteFiles;

	// 日志开始时间
	private long startTimeLog;

	// 每次解析和高级运算的时间
	private long perParseTime;

	// 总共解析和高级运算的时间
	private long totalParseTime;

	// 直接输出的输出字段默认值
	Map<String, String> outColumnDefaultValue;

	private int fileIndex = -1;

	// 当一个map处理多个文件时，上一个文件读取到的记录数
	long perFileRecordCount;

	// 当一个map处理多个文件时，上一个文件无效的记录数
	long perFileRecordInvalidCount;

	// 当一个map处理多个文件时，上一个文件输出的记录总数
	long perFileOutCount;

	// 当前已经成功的文件索引(ture：表示本次任务已经被处理成功)
	boolean isCurrentFileIndexStatus = false;

	/**
	 * 针对直接生成文件功能
	 */
	// 分区字段名称
	private String[] partitionField;

	// 采集插件对象
	private IRemotePlugin remotePlugin;

	@Override
	protected void map(Object key, BytesRefArrayWritable val, Context context) throws IOException, InterruptedException {
		// 监听任务节点
		if (this.isNeedZKMonitor && this.inputMapEnd && null != this.monitorZKNode) {
			this.monitorZKNode.monitorTask();
		}

		// 实际运行标识
		if (!this.isRun) {
			// 日志
			if (MRLOG.queryJOBMapRunLogByMapTaskId(this.taskId) <= 0) {
				MRLOG.addJobMapRun(this.taskId, this.jobLogId, this.recordCount, this.outCount, new Date(), new Date(),
						this.runflag, "start");
			}
			this.isRun = true;
		}

		int temp = 0;
		long tempFileDataLogId = -1;
		this.inputSplit = context.getInputSplit();
		if (this.inputSplit instanceof FileSplit) {
			FileSplit fileSplit = (FileSplit) this.inputSplit;
			temp = fileSplit.getFileIndex();
			tempFileDataLogId = fileSplit.getFileLogId();
			long totalfiles = dbconf.getConf().getLong(NUM_INPUT_FILES, 0);
			context.setStatus("index:" + temp + " mapFiles:" + fileSplit.getFileMaxIndex() + " jobFiles=" + totalfiles +
					" hosts:" + StringUtil.parseArrayToString(fileSplit.getLocations(), ","));
		}

		this.isCurrentFileIndexStatus = false;
		if (this.fileIndex != temp) {
			// 获取当前文件处理是否成功
			if (MRLOG.getMRJobMapDataLogSingleFile(tempFileDataLogId, this.jobLogId) == 2) {// 2:成功
				this.isCurrentFileIndexStatus = true;
			}

			if (this.fileIndex != -1) {// 更新上一个处理成功的日志
				this.updateMRDataLog(this.runflag == 1 ? MRJobMapDataLog.STATUS_SUCCESS : MRJobMapDataLog.STATUS_FAILD);
				// 初始化下一个文件记录
				this.perFileOutCount = 0;
				this.perFileRecordCount = 0;
				this.perFileRecordInvalidCount = 0;
				this.perFileSkipRowNum = this.skipRowNum;
			}
			this.fileIndex = temp;
			FileSplit fileSplit = (FileSplit) this.inputSplit;
			this.fileDataLogId = fileSplit.getFileLogId();
			this.filePath = fileSplit.getPath().toUri().getPath();
			System.out.println("this.fileIndex=" + temp + " filePath=" + fileSplit.getPath().toString());
			String hosts[] = fileSplit.getLocations();
			String fileName = fileSplit.getPath().toUri().getPath().substring(this.filePath.lastIndexOf("/") + 1);
			this.handleMRBefore(fileSplit);// 执行前的预处理

			if (null != this.filterFileRootPath && this.filterFileRootPath.trim().length() > 0 &&
					this.filterIllegalDataWriteFiles) {
				this.fileterFilePath = this.filterFileRootPath + HDFSUtils.getFilterFileName(this.taskId, fileName);
				this.filterFSOStream = HDFSUtils.getFileOStream(this.dbconf.getConf(), fileterFilePath);
			}
		}

		if (this.isCurrentFileIndexStatus) {// 文件处理已成功，不重新处理
			return;
		}

		// 打印日志
		if (this.recordCount % this.perInputNumber == 0) {
			String msg = "map rowtoals:" + this.recordCount + " total time:" +
					(System.currentTimeMillis() - this.startTimeLog) + " per row:" + this.perInputNumber +
					" totalParsetime:" + this.totalParseTime;
			MRLog.systemOut(msg);
		}

		// 文件跳过的初始行数
		if (this.perFileSkipRowNum > 0) {
			this.perFileSkipRowNum--;
			return;
		}

		this.recordCount++;
		this.perFileRecordCount++;

		// 日志
		if (this.recordCount % this.perInputNumber == 0) {
			String msg = "input:" + this.recordCount + " row.";
			MRLog.debug(LOG, msg);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
		}

		try {
			this.perParseTime = System.currentTimeMillis();
			DBRecord value = this.rcfileParse.parseToDBRecord(val);

			// 打印日志记录
			debugInfo("输入:" + value.toString());
			// 插件处理
			List<Map<String, Object>> listVal = IRemotePlugin.mapPlugin(this.remotePlugin, value.getRow());
			if (listVal == null || listVal.size() == 0) {
				this.recordInvalidCount++;
				this.perFileRecordInvalidCount++;
				debugInfo("该记录为非法");
				return;
			}
			for (int x = 0; x < listVal.size(); x++) {
				DBRecord record = new DBRecord(listVal.get(x));
				record.setStatus(value.isStatus());

				// 高级应用处理
				if (this.mrSeniorApply != null) {
					for (int j = 0; j < this.mrSeniorApply.length; j++) {
						if (null != this.mrSeniorApply[j]) {
							this.mrSeniorApply[j].apply(record, context.getInputSplit());
						}
					}
				}

				this.totalParseTime += System.currentTimeMillis() - this.perParseTime;

				// 无效
				if (!record.isStatus()) {
					if (this.filterIllegalDataWriteFiles) {
						HDFSUtils.write(this.filterFSOStream, record.toString());
					}
					return;
				}

				// 只过滤
				if (this.onlyFilterNotStorage) {
					return;
				}

				if (this.inputMapEnd) {
					// 直接输出
					DBRecord _record = new DBRecord();
					_record.getRow().putAll(this.outColumnDefaultValue);// 优先设置默认值（输出的默认值）
					_record.addChange(record, this.destFieldNames, this.destRelSrcFieldNames);
					DBRecord[] gps = record.splitPartition(this.destFieldNames, this.destRelSrcFieldNames,
							this.partitionField);
					if (null == gps) {
						IRemotePlugin.beforConvertRow(this.remotePlugin, record);
						context.write(record, null);
						// 打印日志记录
						this.debugInfo("输出:" + record.toString());
					} else {
						IRemotePlugin.beforConvertRow(this.remotePlugin, gps[0]);
						IRemotePlugin.beforConvertRow(this.remotePlugin, gps[1]);
						context.write(gps[0], gps[1]); // 分区输出
						// 打印日志记录
						this.debugInfo("输出: 分区字段信息=>" + gps[0].toString());
					}
				} else {
					DBRecord[] gps = record.splitGroup(this.destFieldNames, this.destRelSrcFieldNames,
							this.groupFieldMethod);
					IRemotePlugin.beforConvertRow(this.remotePlugin, gps[0]);
					IRemotePlugin.beforConvertRow(this.remotePlugin, gps[1]);
					context.write(gps[0], gps[1]);

					// 打印日志记录
					this.debugInfo("输出: 分组信息=>" + gps[0].toString() + " 统计信息=>" + gps[0].toString());
				}
				this.outCount++;
				this.perFileOutCount++;

				// 日志
				if (this.outCount % this.perOutputNumber == 0) {
					String msg = "output:" + this.recordCount + " row.";
					MRLog.debug(LOG, msg);
					MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
				}
			}
		} catch (Exception e) {
			// 异常设置任务节点失败
			if (this.isNeedZKMonitor && this.inputMapEnd && null != this.monitorZKNode) {
				try {
					this.monitorZKNode.setZKNodeData(MRWatcher.FAILED.getBytes());
				} catch (Exception e1) {
					String msg = "set zookeeper taskNode FAILED value exception!";
					MRLog.errorException(LOG, msg, e1);
					MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(),
							msg + " exception:" + StringUtil.stringifyException(e));
				}
			}

			// 设置运行异常标识符
			this.runflag = 2;

			// 输出日志信息
			String msg = "exception:" + StringUtil.stringifyException(e);
			MRLog.errorException(LOG, msg, e);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);

			throw new IOException(e);
		}
	}

	/**
	 * 执行预处理操作
	 * 
	 * @param fileSplit
	 */
	private void handleMRBefore(FileSplit fileSplit) {
		boolean isDeleteBeforeData = fileSplit.isDeleteBeforeData();
		String fileId = fileSplit.getFileId();
		if (!isDeleteBeforeData) {
			return;
		}

		this.mrBefore.setFileId(fileId);
		try {
			this.mrBefore.before(this.dbconf.getConf());
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_INFO, new Date(), "delete before data success");
		} catch (Exception e) {
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), "delete before data fail", e);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.dbconf = new FileConfiguration(context.getConfiguration(), FileConfiguration.FLAG_FILE_INPUT);
		this.jobId = this.dbconf.getSysJobId();
		this.sysJobId = this.dbconf.getConf().get("mapred.job.id");
		this.taskId = this.dbconf.getConf().get("mapred.tip.id"); // 获取任务id
		this.jobLogId = this.dbconf.getJobLogId(); // 获取job的日志id
		this.perInputNumber = this.dbconf.getJOBLogMapInputPerRecordNumber(); // 实时记录输入数据量
		this.perOutputNumber = this.dbconf.getJOBLogMapOutputPerRecordNumber(); // 实时记录输入数据量

		this.inputMapEnd = this.dbconf.getInputMapEnd();// 是否map直接输出结果
		this.srcFieldNames = this.dbconf.getInputFieldNames();// 输入字段
		this.rowSplitChars = this.dbconf.getInputFileRowsSplitChars();// 行分隔符
		this.fieldSplitChars = this.dbconf.getInputFileFieldSplitChars();// 列分隔符
		String[] targetFieldNames = this.dbconf.getOutputFieldNames();// 输出
		this.groupFieldMethod = this.dbconf.getRelatedGroupFieldMethod();// 目标到源映射及方法
		this.skipRowNum = this.dbconf.getInputFileSkipRowNum(); // 设置文件跳过的行数
		this.perFileSkipRowNum = this.skipRowNum;
		this.onlyFilterNotStorage = this.dbconf.getInputMapOnlyFilterNotStorage();// 只过滤，不入库的标示
		this.filterIllegalDataWriteFiles = this.dbconf.getInputMapFilterIllegalDatawriteFiles();// 数据过滤掉的数据不写文件
		this.outColumnDefaultValue = StringUtil.decodeOutColumnDefaultValue(this.dbconf.getOutputColumnDefaultValue());// 获取直接输出的输出字段的默认值
		HiveConfiguration hconf = new HiveConfiguration(context.getConfiguration());
		this.partitionField = hconf.getOutputHivePartitionField(); // hive分区字段

		this.debugRowNum = this.dbconf.getJobLogDebugRowNum();
		this.debug = this.dbconf.getJobLogDebug();

		// 若未指定输出字段，直接将输入字段作为输出字段
		if (null == targetFieldNames) {
			targetFieldNames = this.srcFieldNames.clone();
		}

		// 打印级别1
		if (this.debug >= MRConfiguration.INTERNAL_JOB_LOG_DEBUG_CONF_INFO) {
			StringBuffer orginConf = new StringBuffer();
			orginConf.append("\n实时记录输入数据量:" + this.perInputNumber + "\n");
			orginConf.append("实时记录输入数据量:" + this.perOutputNumber + "\n");
			orginConf.append("输入字段:" + StringUtil.toString(this.srcFieldNames, null) + "\n");
			orginConf.append("输出字段:" + StringUtil.toString(targetFieldNames, null) + "\n");
			orginConf.append("目标到源映射及方法:" + StringUtil.toString(this.groupFieldMethod, null) + "\n");
			orginConf.append("初始跳过的行数:" + this.skipRowNum + "\n");
			MRLog.systemOut(orginConf.toString());
		}

		Set<String> setSrcFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setSrcFieldNames, this.srcFieldNames);

		Set<String> setTargetFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setTargetFieldNames, targetFieldNames);

		if (this.inputMapEnd) {
			// map直接输出拆分字段
			this.noReduceAnalyzeConfig(dbconf.getConf(), setSrcFieldNames, setTargetFieldNames);
		} else {
			this.analyzeConfig(dbconf.getConf(), setSrcFieldNames, setTargetFieldNames);
		}

		// 初始化rc解析类
		this.rcfileParse = new RCFileParse(this.srcFieldNames);

		// 初始化日志信息
		MRLog.getInstance().setConf(this.dbconf);

		// 设置zk监听参数
		this.isNeedZKMonitor = this.dbconf.isZKMonitor();
		this.monitor(this.dbconf);

		// 初始化高级应用对象
		this.senior(this.dbconf);

		// 获取过滤数据存放的根路径
		this.filterFileRootPath = MRSeniorUtil.getFilterRootPath(this.dbconf, this.mrSeniorApply);
		if (null != this.filterFileRootPath) {
			this.filterFileRootPath = this.filterFileRootPath.endsWith("/") ? this.filterFileRootPath
					: this.filterFileRootPath + "/";
		}

		// 初始化插件
		this.remotePlugin = IRemotePlugin.configurePlugin(context, this);

		// 打印级别2
		if (this.debug >= MRConfiguration.INTERNAL_JOB_LOG_DEBUG_DECONF_INFO) {
			StringBuffer deConf = new StringBuffer();
			deConf.append("\n任务id:" + this.taskId + "\n");
			deConf.append("JOB的日志ID:" + this.jobLogId + "\n");
			deConf.append("是否map直接输出结果:" + this.inputMapEnd + "\n");
			deConf.append("处理后的输入字段:" + StringUtil.toString(this.destFieldNames, null) + "\n");
			deConf.append("处理后的输出字段:" + StringUtil.toString(this.destRelSrcFieldNames, null) + "\n");
			deConf.append("处理后的目标到源映射及方法:" + StringUtil.toString(this.groupFieldMethod, null) + "\n");
			deConf.append("过滤数据存放的文件路径:" + this.filterFileRootPath + "\n");
			deConf.append("输出字段的默认值:" + this.outColumnDefaultValue + "\n");
			if (this.partitionField != null && this.partitionField.length > 0) {
				deConf.append("分区字段:" + StringUtil.toString(this.partitionField, ",") + "\n");
			}
			MRLog.systemOut(deConf.toString());
		}
	}

	/**
	 * 分析配置信息
	 * 
	 * @param job
	 *            job对象
	 * @param setSrcFieldNames
	 *            输入字段
	 * @param setTargetFieldNames
	 *            输出字段
	 */
	private void analyzeConfig(Configuration job, Set<String> setSrcFieldNames, Set<String> setTargetFieldNames) {
		this.destFieldNames = new String[this.groupFieldMethod.length];
		this.destRelSrcFieldNames = new String[this.groupFieldMethod.length];
		for (int i = 0; i < this.groupFieldMethod.length; i++) {
			String[] tmp = this.groupFieldMethod[i].split(":");
			if (tmp.length != 3) {
				String msg = "分组计算中目标到源映射及方法配置不正确:" + job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setTargetFieldNames.contains(tmp[0])) {
				String msg = "配置错误,目标字段" + tmp[0] + "不存在于输出字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setSrcFieldNames.contains(tmp[1])) {
				String msg = "配置错误,源字段" + tmp[1] + "不存在输入字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			this.destFieldNames[i] = tmp[0];
			this.destRelSrcFieldNames[i] = tmp[1];
			this.groupFieldMethod[i] = tmp[2];
		}
	}

	/**
	 * Map直接输出的分析配置信息
	 * 
	 * @param job
	 * @param setSrcFieldNames
	 * @param setTargetFieldNames
	 */
	private void noReduceAnalyzeConfig(Configuration job, Set<String> setSrcFieldNames, Set<String> setTargetFieldNames) {
		if (null == this.groupFieldMethod) {
			return;
		}

		if (this.groupFieldMethod.length <= 0) {
			String msg = "目标到源字段配置为空:" + job.get(DBConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
			throw new RuntimeException(msg);
		}

		this.destFieldNames = new String[this.groupFieldMethod.length];
		this.destRelSrcFieldNames = new String[this.groupFieldMethod.length];
		for (int i = 0; i < this.groupFieldMethod.length; i++) {
			String tts[] = this.groupFieldMethod[i].split(":");
			if (!(tts.length == 2 || tts.length == 3)) {
				String msg = "目标到源字段配置不正确:" + job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setTargetFieldNames.contains(tts[0])) {
				String msg = "配置错误,目标字段" + tts[0] + "不存在于输出字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setSrcFieldNames.contains(tts[1])) {
				String msg = "配置错误,源字段" + tts[1] + "不存在输入字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}
			this.destFieldNames[i] = tts[0];
			this.destRelSrcFieldNames[i] = tts[1];
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		MRLog.consoleDebug(LOG, "Total: RCFileMap read " + this.recordCount + " rows, write" + this.outCount + " rows.");
		// map已经实际运行过
		if (this.isRun) {
			// 日志
			String msg = this.runflag == 1 ? "sucess" : "failed";
			// 更新mr的数据记录
			this.updateMRDataLog(this.runflag == 1 ? MRJobMapDataLog.STATUS_SUCCESS : MRJobMapDataLog.STATUS_FAILD);
			MRLOG.updateJobMapRun(this.taskId, this.jobLogId, this.recordCount, this.outCount, null, new Date(),
					this.runflag, msg, this.recordInvalidCount);

			// 添加到记录数
			MRLOG.updateMapCountJobRun(this.jobLogId, this.recordCount, this.outCount, this.recordInvalidCount);
		}
		IRemotePlugin.closePlugin(this.remotePlugin);
	}

	@Override
	public void monitor(MRConfiguration conf) {
		String zkTaskPath = null;
		if (this.inputMapEnd && this.isNeedZKMonitor) {
			String zkAddress = conf.getZKAddress();
			String zkRootPath = conf.getZKMapperPath();
			zkTaskPath = zkRootPath + "/" + this.taskId;
			this.monitorZKNode = new MonitorZKNode(this.taskId, zkTaskPath, zkAddress);

			String msg = "RCFileMapper monitor task nodePath is:" + zkTaskPath;
			MRLog.infoZK(LOG, msg);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_INFO, new Date(), msg);
		}
	}

	@Override
	public void senior(MRConfiguration conf) {
		if (null == conf) {
			return;
		}

		String sconf = conf.getInputMapSeniorConf();
		if (null == sconf || sconf.length() <= 0) {
			return;
		}

		this.mrSeniorApply = MRSeniorUtil.getMRSeniorApplies(sconf, conf.getConf(), 0);
	}

	/**
	 * 更新日志信息
	 * 
	 * @param reporter
	 */
	private void updateMRDataLog(int status) {
		MRJobMapDataLog dataLog = new MRJobMapDataLog();
		dataLog.setEndTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		dataLog.setFailCount(this.perFileRecordCount - this.perFileOutCount);
		dataLog.setStatus(status);
		dataLog.setSuccessCount(this.perFileOutCount);
		dataLog.setTotalCount(this.perFileRecordCount);
		dataLog.setFilterPath(this.fileterFilePath);
		dataLog.setId(this.fileDataLogId);
		MRLOG.updateMRJobMapDataLog(dataLog);
	}

	/**
	 * 打印记录的日志信息
	 * 
	 * @param value
	 *            日志信息
	 */
	private void debugInfo(String value) {
		// 打印级别3
		if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD) {
			if (this.debugRowNum >= this.recordCount) {
				MRLog.consoleDebug(LOG, value);
			}
		}
	}

}
