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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;

import com.ery.hadoop.mrddx.before.FileMapperBefore;
import com.ery.hadoop.mrddx.db.DBOutputFormat;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.FileSplit;
import com.ery.hadoop.mrddx.file.TextParse;
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


 * 

 * @version v1.0
 * @create Data 2013-1-9
 */
public class FileMapper extends Mapper<Object, Text, DBRecord, DBRecord> implements IDBMonitor, IDBSenior {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(FileMapper.class);
	public static final MRLog MRLOG = MRLog.getInstance();
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	public int debug = -1; // 日志级别 1：打印job配置参数,
	// 2:打印job解析后的参数(包含1)，3：打印map和redue的输入输出记录
	public int debugRowNum = -1; // 每个map输入条数限制(设置-debug参数时，才生效, 默认值：1000)

	// map的拆分对象
	public InputSplit inputSplit;

	/**
	 * 日志参数
	 */
	// map执行是否成功, 取值范围[1-2]:1-成功,2-失败
	public int runflag = 1;

	// job的日志id号
	public long jobLogId;

	// 当前map是否实际运行过(true-运行过,false-未运行过)
	public boolean isRun = false;

	// 输出的记录数
	public long outCount;

	// 实时记录输入数据量
	public long perInputNumber;

	// 实时记录输出数据量
	public long perOutputNumber;

	// 记录数据的日志id
	public long fileDataLogId;

	// 过滤数据存放的根路径
	public String filterFileRootPath;

	// 过滤数据的输出流
	public FSDataOutputStream filterFSOStream;

	// 文件路径
	public String filePath;

	// 文件的过滤数据存放路径
	public String fileterFilePath = "";

	/**
	 * 监听业务参数
	 */
	// 监听节点对象
	public IMonitorZKNode monitorZKNode;

	// 是否需要监控
	public boolean isNeedZKMonitor;

	/**
	 * 执行MAP前的预处理参数
	 */
	public FileMapperBefore mrBefore;

	/**
	 * 执行任务的常规配置
	 */
	// 配置信息
	public FileConfiguration dbconf;

	// jobID
	public long jobId;

	// 系统运行的jobID
	public String sysJobId;

	// 任务ID
	public String taskId;

	// 读取到的记录数
	public long recordCount;

	// 无效的记录数
	public long recordInvalidCount;

	// 是否map直接输出结果（true：直接输出）
	public boolean inputMapEnd;

	// 输入字段名称
	public String[] srcFieldNames;

	// 拆分列的分隔符
	public String fieldSplitChars;

	// 拆分行记录分隔符
	public String rowSplitChars;

	// 目标字段名称
	public String[] destFieldNames;

	// 源字段名称
	public String[] destRelSrcFieldNames;

	// 字段方法名称
	public String[] groupFieldMethod;

	// text解析类
	public TextParse textParse;

	// 高级应用对象
	public IMRSeniorApply mrSeniorApply[];

	// 每个文件开头跳过的行数
	public int perFileSkipRowNum = -1;

	// 存放文件开头跳过的行数
	public int skipRowNum = -1;

	// 数据只过滤，不入库标示符，true:真
	public boolean onlyFilterNotStorage;

	// 数据过滤掉的数据写文件，true:过滤掉的数据写文件
	public boolean filterIllegalDataWriteFiles;

	// 日志开始时间
	public long startTimeLog;

	// 每次解析和高级运算的时间
	public long perParseTime;

	// 总共解析和高级运算的时间
	public long totalParseTime;

	// 直接输出的输出字段默认值
	public Map<String, String> outColumnDefaultValue;

	public int fileIndex = 0;

	// 当一个map处理多个文件时，上一个文件读取到的记录数
	public long perFileRecordCount;

	// 当一个map处理多个文件时，上一个文件无效的记录数
	public long perFileRecordInvalidCount;

	// 当一个map处理多个文件时，上一个文件输出的记录总数
	public long perFileOutCount;

	// 当前已经成功的文件索引(ture：表示本次任务已经被处理成功)
	public boolean isCurrentFileIndexStatus = false;

	/**
	 * 针对直接生成文件功能
	 */
	// 分区字段名称
	public String[] partitionField;
	public String fileEncodeing = "utf-8";
	public boolean fileEncodeingIsUTF8 = true;
	// 采集插件对象
	public IRemotePlugin remotePlugin;

	public long readNum = 0;
	RecordReader realReader;

	@Override
	protected void map(Object key, Text val, Context context) throws IOException, InterruptedException {
		// 实际运行标识
		if (!this.isRun) {
			// 监听任务节点
			if (this.isNeedZKMonitor && this.inputMapEnd && null != this.monitorZKNode) {
				this.monitorZKNode.monitorTask();
			}
			// 新增日志
			this.startTimeLog = System.currentTimeMillis();
			if (MRLOG.queryJOBMapRunLogByMapTaskId(this.taskId) <= 0) {
				MRLOG.addJobMapRun(this.taskId, this.jobLogId, this.recordCount, this.outCount, new Date(), new Date(),
						this.runflag, "start");
			}
			FileSplit fileSplit = (FileSplit) this.inputSplit;
			this.handleMRBefore(fileSplit);// 执行前的预处理
			this.isRun = true;
		}
		int temp = 0;
		long tempFileDataLogId = -1;
		FileSplit fileSplit = (FileSplit) this.inputSplit;
		temp = fileSplit.getFileIndex();
		tempFileDataLogId = fileSplit.getFileLogId();
		long totalfiles = dbconf.getConf().getLong(NUM_INPUT_FILES, 0);
		if (this.recordCount % 1000 == 0)
			context.setStatus("index:" + temp + " mapFiles:" + fileSplit.getFileMaxIndex() + " jobFiles=" + totalfiles +
					" recordCount:" + this.recordCount);

		this.isCurrentFileIndexStatus = false;
		if (this.fileIndex != temp) {
			// 获取当前文件处理是否成功
			if (MRLOG.getMRJobMapDataLogSingleFile(tempFileDataLogId, this.jobLogId) == 2) {// 2:成功
				this.isCurrentFileIndexStatus = true;
			}
			// 更新上一个处理成功的日志
			this.updateMRDataLog(this.runflag == 1 ? MRJobMapDataLog.STATUS_SUCCESS : MRJobMapDataLog.STATUS_FAILD);
			// 初始化下一个文件记录
			this.runflag = 1;
			this.perFileOutCount = 0;
			this.perFileRecordCount = 0;
			this.perFileRecordInvalidCount = 0;
			if (fileSplit.getStart() == 0)
				this.perFileSkipRowNum = this.skipRowNum;
			else
				this.perFileSkipRowNum = 0;
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
		if (this.perInputNumber != 0 && this.readNum > 0 && this.readNum % this.perInputNumber == 0) {
			String msg = "map rowtoals:" + this.recordCount + " total time:" +
					(System.currentTimeMillis() - this.startTimeLog) + " per row:" + this.perInputNumber +
					" totalParsetime:" + this.totalParseTime;
			LOG.info(msg);
		}
		if (readNum++ < 5) {
			if (fileEncodeingIsUTF8) {// 编码转换
				LOG.info("map=" + val);
			} else {
				LOG.info("map=" +
						new String(new String(val.getBytes(), 0, val.getLength(), this.fileEncodeing)
								.getBytes(MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT),
								MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT));
			}
		}
		// 文件跳过的初始行数
		if (this.perFileSkipRowNum > 0) {// 文件拆分后中间的不需要
			context.getCounter("fileInput Counter", "FileSkipRowNum").increment(1);
			this.perFileSkipRowNum--;
			if (key instanceof LongWritable) {
				if (((LongWritable) key).get() < (1 << 15)) {
					return;
				}
			} else if (key instanceof IntWritable) {
				if (((LongWritable) key).get() < (1 << 15)) {
					return;
				}
				// } else {
				// return;
			}
		}

		try {
			String _val = null;
			if (fileEncodeingIsUTF8) {// 编码转换
				_val = val.toString();
			} else {
				byte[] bts = new String(val.getBytes(), 0, val.getLength(), this.fileEncodeing)
						.getBytes(MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT);
				_val = new String(bts, MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT);
			}
			_val = IRemotePlugin.line(this.remotePlugin, _val);
			// 转化文本数据为Mapper的输出的key-value对的格式(DBRecord)
			this.perParseTime = System.currentTimeMillis();
			DBRecord value[] = this.textParse.parseToDBRecord(_val);
			if (value == null) {
				context.getCounter("Map-Reduce Framework", "Map input null records").increment(1);
				this.recordInvalidCount++;
				this.perFileRecordInvalidCount++;
				return;
			}
			if (this.debug > 0 && this.debugRowNum >= 0 && this.debugRowNum > this.perFileRecordCount) {
				return;
			}

			for (int i = 0; i < value.length; i++) {// 日志
				this.recordCount++;
				this.perFileRecordCount++;
				if (this.recordCount % this.perInputNumber == 0) {
					String msg = "input:" + this.recordCount + " row," + " filter:" + recordInvalidCount + " row.";
					MRLog.info(LOG, msg);
					MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
				}
				// 打印日志记录
				if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD &&
						this.debugRowNum >= this.perFileRecordCount)
					debugInfo("输入:" + value[i].toString());

				// 插件处理
				List<Map<String, Object>> listVal = IRemotePlugin.mapPlugin(this.remotePlugin, value[i].getRow());
				if (listVal == null || listVal.size() == 0) {
					this.recordInvalidCount++;
					this.perFileRecordInvalidCount++;
					if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD &&
							this.debugRowNum >= this.perFileRecordCount)
						debugInfo("该记录为非法");
					continue;
				}

				for (int j = 0; j < listVal.size(); j++) {
					DBRecord record = new DBRecord(listVal.get(j));
					record.setStatus(value[i].isStatus());
					// 高级应用处理
					if (this.mrSeniorApply != null) {
						for (int n = 0; n < this.mrSeniorApply.length; n++) {
							if (null != this.mrSeniorApply[n]) {
								this.mrSeniorApply[n].apply(record, this.inputSplit);
							}
						}
					}
					if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD &&
							this.debugRowNum >= this.perFileRecordCount)
						debugInfo("输入 Senior:" + record.toString());
					this.totalParseTime += System.currentTimeMillis() - this.perParseTime;

					// 无效
					if (!record.isStatus()) {
						if (this.filterIllegalDataWriteFiles) {
							HDFSUtils.write(this.filterFSOStream, record.toString());
						}
						continue;
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

						DBRecord[] gps = _record.splitPartition(this.destFieldNames, this.destRelSrcFieldNames,
								this.partitionField);
						if (null == gps) {
							// output.collect(record, null);
							IRemotePlugin.beforConvertRow(this.remotePlugin, _record);
							context.write(_record, null);
							// 打印日志记录
							if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD &&
									this.debugRowNum >= this.perFileRecordCount)
								this.debugInfo("输出:" + _record.toString());
						} else {
							// output.collect(gps[0], gps[1]); // 分区输出
							IRemotePlugin.beforConvertRow(this.remotePlugin, _record);
							IRemotePlugin.beforConvertRow(this.remotePlugin, gps[1]);
							context.write(gps[0], gps[1]); // 分区输出
							// 打印日志记录
							if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD &&
									this.debugRowNum >= this.perFileRecordCount)
								this.debugInfo("输出: 分区字段信息=>" + gps[0].toString());
						}
					} else {
						DBRecord[] gps = record.splitGroup(this.destFieldNames, this.destRelSrcFieldNames,
								this.groupFieldMethod);
						context.write(gps[0], gps[1]);
						// 打印日志记录
						if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD &&
								this.debugRowNum >= this.perFileRecordCount)
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
			String msg = "";
			if (val != null && val.getLength() > 1000) {
				msg = "input value=>>" + val.toString().substring(0, 1000) + ",exception:" +
						StringUtil.stringifyException(e);
			} else {
				msg = "input value=>>null, " + " exception:" + StringUtil.stringifyException(e);
			}
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
		this.fileIndex = fileSplit.getFileIndex();
		this.fileDataLogId = fileSplit.getFileLogId();
		this.filePath = fileSplit.getPath().toUri().getPath();
		LOG.info("fileIndex=" + this.fileIndex + " filePath=" + fileSplit.getPath().toString() + " " +
				fileSplit.getStart() + " : " + fileSplit.getLength() + " fileDataLogId=" + fileDataLogId);

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
		Configuration job = context.getConfiguration();
		this.realReader = IRemotePlugin.getRealInputStream(context);

		this.dbconf = new FileConfiguration(context.getConfiguration(), FileConfiguration.FLAG_FILE_INPUT);
		this.jobId = this.dbconf.getSysJobId();
		this.sysJobId = this.dbconf.getConf().get("mapred.job.id");
		this.taskId = this.dbconf.getConf().get("mapred.tip.id"); // 获取任务id
		this.jobLogId = this.dbconf.getJobLogId(); // 获取job的日志id
		this.perInputNumber = this.dbconf.getJOBLogMapInputPerRecordNumber(); // 实时记录输入数据量
		this.perOutputNumber = this.dbconf.getJOBLogMapOutputPerRecordNumber(); // 实时记录输入数据量

		this.fileEncodeing = this.dbconf.getConf()
				.get(MRConfiguration.FILE_CONTENT_ENCODING, MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT)
				.toLowerCase();
		if (this.fileEncodeing.equals("")) {
			this.fileEncodeing = "utf-8";
		}
		this.fileEncodeingIsUTF8 = this.fileEncodeing.equals(MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT);

		this.inputMapEnd = this.dbconf.getInputMapEnd();// 是否map直接输出结果
		this.srcFieldNames = this.dbconf.getInputFieldNames();// 输入字段
		this.rowSplitChars = this.dbconf.getInputFileRowsSplitChars();// 行分隔符
		this.fieldSplitChars = this.dbconf.getInputFileFieldSplitChars();// 列分隔符
		String[] targetFieldNames = this.dbconf.getOutputFieldNames();// 输出
		this.groupFieldMethod = this.dbconf.getRelatedGroupFieldMethod();// 目标到源映射及方法
		this.skipRowNum = this.dbconf.getInputFileSkipRowNum(); // 设置文件跳过的行数
		this.perFileSkipRowNum = this.skipRowNum;
		this.inputSplit = context.getInputSplit();
		FileSplit fileSplit = (FileSplit) this.inputSplit;
		if (fileSplit.getStart() == 0)
			this.perFileSkipRowNum = this.skipRowNum;
		else
			this.perFileSkipRowNum = 0;
		LOG.info("set skipRowNum=" + skipRowNum);
		this.onlyFilterNotStorage = this.dbconf.getInputMapOnlyFilterNotStorage();// 只过滤，不入库的标示
		this.filterIllegalDataWriteFiles = this.dbconf.getInputMapFilterIllegalDatawriteFiles();// 数据过滤掉的数据写文件
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
			this.noReduceAnalyzeConfig(job, setSrcFieldNames, setTargetFieldNames);
		} else {
			this.analyzeConfig(job, setSrcFieldNames, setTargetFieldNames);
		}

		this.textParse = new TextParse(this.srcFieldNames, this.fieldSplitChars, this.rowSplitChars, this.taskId);

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

		// 初始化预处理操作
		this.mrbefore(this.dbconf);

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
	 *            job对象
	 * @param setSrcFieldNames
	 *            输入字段
	 * @param setTargetFieldNames
	 *            输出字段
	 */
	private void noReduceAnalyzeConfig(Configuration job, Set<String> setSrcFieldNames, Set<String> setTargetFieldNames) {
		if (null == this.groupFieldMethod) {
			return;
		}

		if (this.groupFieldMethod.length <= 0) {
			String msg = "目标到源字段配置为空:" + job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
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
		context.getCounter("fileInput Counter", "readNum").increment(readNum);
		context.getCounter("fileInput Counter", "recordCount").increment(recordCount);
		context.getCounter("fileInput Counter", "outCount").increment(outCount);
		context.getCounter("fileInput Counter", "recordInvalidCount").increment(recordInvalidCount);

		MRLog.consoleDebug(LOG, "Total: FileMap read " + readNum + "  splitRecord:" + this.recordCount +
				" rows, writeRecord:" + this.outCount + " rows." + " filter:" + perFileRecordInvalidCount);
		// map已经实际运行过
		if (this.isRun) {
			// 日志
			String msg = "input:" + this.recordCount + " row," + " filter:" + perFileRecordInvalidCount + " row.";
			MRLog.info(LOG, msg);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
			msg = this.runflag == 1 ? "sucess" : "failed";
			// 更新mr的数据记录
			LOG.info("updateMRDataLog setFailCount:" + (this.perFileRecordCount - this.perFileOutCount) +
					" setStatus:" +
					(this.runflag == 1 ? MRJobMapDataLog.STATUS_SUCCESS : MRJobMapDataLog.STATUS_FAILD) +
					" setSuccessCount:" + (this.perFileOutCount) + " setTotalCount:" + (this.perFileRecordCount) +
					" setFilterPath:" + (this.fileterFilePath) + " setId:" + this.fileDataLogId);

			this.updateMRDataLog(this.runflag == 1 ? MRJobMapDataLog.STATUS_SUCCESS : MRJobMapDataLog.STATUS_FAILD);
			MRLOG.updateJobMapRun(this.taskId, this.jobLogId, this.recordCount, this.outCount, null, new Date(),
					this.runflag, msg, this.recordInvalidCount);
			// 添加到记录数
			MRLOG.updateMapCountJobRun(this.jobLogId, this.recordCount, this.outCount, this.recordInvalidCount);
		}
		super.cleanup(context);
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

			String msg = "FileMapper monitor task nodePath is:" + zkTaskPath;
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
	 * 预处理操作
	 * 
	 * @param dbconf2
	 */
	private void mrbefore(FileConfiguration dbconf) {
		this.mrBefore = new FileMapperBefore();
		String[] outputType = dbconf.getOutputFormatType();
		for (int i = 0; i < outputType.length; i++) {
			String outputClass = dbconf.getOutputFormatClassName(outputType[i]);
			if (DBOutputFormat.class.getCanonicalName().equalsIgnoreCase(outputClass)) {
				this.mrBefore.setOpType(outputType[i]);
				break;
			}
		}
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
				LOG.info(MRLog.LOG_PREFIX_DEBUG + value);
				// MRLog.consoleDebug(LOG, value);
			}
		}
	}

}
