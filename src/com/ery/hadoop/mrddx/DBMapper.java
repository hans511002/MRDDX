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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;

import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.hive.HiveConfiguration;
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
 * 处理value为DBRecord的Mapper Copyrights @ 2012,Tianyuan DIC Information Co.,Ltd.
 * All rights reserved.
 * 
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 */
public class DBMapper extends Mapper<LongWritable, DBRecord, DBRecord, DBRecord>
// extends MapReduceBase Mapper<LongWritable, DBRecord, DBRecord, DBRecord>
		implements IDBMonitor, IDBSenior {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(DBMapper.class);
	public static final MRLog MRLOG = MRLog.getInstance();
	public int debug = -1; // 日志级别 1：打印job配置参数,
	// 2:打印job解析后的参数(包含1)，3：打印map和redue的输入输出记录
	public int debugRowNum = -1; // 每个map输入条数限制(设置-debug参数时，才生效, 默认值：1000)

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

	// 实时记录输入数据量
	private long perInputNumber;

	// 实时记录输入数据量
	private long perOutputNumber;

	// 过滤数据存放的根路径
	private String filterFileRootPath;

	// 过滤数据的输出流
	private FSDataOutputStream filterFSOStream;

	// 文件的过滤数据存放路径
	private String fileterFilePath = "";

	/**
	 * 以下为业务参数
	 */
	// 监听节点对象
	private IMonitorZKNode monitorZKNode;

	// 是否需要监控
	private boolean isNeedZKMonitor;

	// 配置信息
	private MRConfiguration dbconf;

	// jobID
	private long jobId;

	// 系统运行的jobID
	private String sysJobId;

	// 任务ID
	private String taskId;

	// 读取到的记录数
	private long recordCount;

	// 无效的记录数
	private long recordInvalidCount;

	// 是否map直接输出结果（true：直接输出）
	private boolean inputMapEnd;

	// 输入字段名称
	private String[] srcFieldNames = null;

	// 目标字段名称
	private String[] destFieldNames = null;

	// 源字段名称
	private String[] destRelSrcFieldNames = null;

	// 字段方法名称
	String[] groupFieldMethod = null;

	// 高级应用对象
	private IMRSeniorApply mrSeniorApply[];

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

	/**
	 * 针对直接生成文件功能
	 */
	// 分区字段名称
	private String[] partitionField;

	// 采集插件对象
	private IRemotePlugin remotePlugin;

	@Override
	public void map(LongWritable key, DBRecord dbRv, Context context) throws IOException {

		// }
		// @Override
		// public void map(LongWritable key, DBRecord value,
		// OutputCollector<DBRecord, DBRecord> output, Reporter reporter)
		// throws IOException {
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
			if (null != this.filterFileRootPath && this.filterFileRootPath.trim().length() > 0 &&
					this.filterIllegalDataWriteFiles) {
				this.fileterFilePath = this.filterFileRootPath + HDFSUtils.getFilterFileName(this.taskId, null);
				this.filterFSOStream = HDFSUtils.getFileOStream(this.dbconf.getConf(), fileterFilePath);
			}
		}

		// 打印日志
		if (this.recordCount % this.perInputNumber == 0) {
			String msg = "map rowtoals:" + this.recordCount + " total time:" +
					(System.currentTimeMillis() - this.startTimeLog) + " per row:" + this.perInputNumber +
					" totalParsetime:" + this.totalParseTime;
			MRLog.systemOut(msg);
		}

		this.recordCount++;
		this.perParseTime = System.currentTimeMillis();

		// 日志
		if (this.recordCount % this.perInputNumber == 0) {
			String msg = "input:" + this.recordCount + " row.";
			MRLog.debug(LOG, msg);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
		}

		// 打印日志记录
		debugInfo("输入:" + dbRv.toString());

		// 插件处理
		List<Map<String, Object>> listVal = IRemotePlugin.mapPlugin(this.remotePlugin, dbRv.getRow());
		if (listVal == null || listVal.size() == 0) {
			this.recordInvalidCount++;
			debugInfo("该记录为非法");
			return;
		}
		for (int x = 0; x < listVal.size(); x++) {
			DBRecord record = new DBRecord(listVal.get(x));
			record.setStatus(dbRv.isStatus());
			// 高级应用处理
			if (this.mrSeniorApply != null) {
				for (int j = 0; j < this.mrSeniorApply.length; j++) {
					if (null != this.mrSeniorApply[j]) {
						this.mrSeniorApply[j].apply(record, null);
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

			try {
				if (this.inputMapEnd) {
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
						this.debugInfo("输出:" + _record.toString());
					} else {
						// output.collect(gps[0], gps[1]); // 分区输出
						context.write(gps[0], gps[1]);
						// 打印日志记录
						this.debugInfo("输出: 分区字段信息=>" + gps[0].toString());
					}
				} else {
					DBRecord[] gps = record.splitGroup(this.destFieldNames, this.destRelSrcFieldNames,
							this.groupFieldMethod);
					// output.collect(gps[0], gps[1]);
					IRemotePlugin.beforConvertRow(this.remotePlugin, gps[0]);
					IRemotePlugin.beforConvertRow(this.remotePlugin, gps[1]);
					context.write(gps[0], gps[1]);
					// 打印日志记录
					this.debugInfo("输出: 分组信息=>" + gps[0].toString() + " 统计信息=>" + gps[0].toString());
				}
				this.outCount++;

				// 日志
				if (this.outCount % this.perOutputNumber == 0) {
					String msg = "output:" + this.recordCount + " row.";
					MRLog.debug(LOG, msg);
					MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
				}
			} catch (Exception e) {
				// 异常设置任务节点失败
				if (this.isNeedZKMonitor && this.inputMapEnd && null != this.monitorZKNode) {
					try {
						this.monitorZKNode.setZKNodeData(MRWatcher.FAILED.getBytes());
					} catch (Exception e1) {
						String msg = "set zookeeper taskNode FAILED value exception!";
						MRLog.errorException(LOG, msg, e1);
						MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg + " exception:" +
								StringUtil.stringifyException(e));
					}
				}
				// 设置运行异常标识符
				this.runflag = 2;
				// 输出日志信息
				String msg = "input value=>>" + record == null ? "" : record.getRow().toString() + " exception:" +
						StringUtil.stringifyException(e);
				MRLog.errorException(LOG, msg, e);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new IOException(e);
			}
		}
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		// // NOTHING
		// }
		// @Override
		// public void steup(JobConf job) {
		Configuration job = context.getConfiguration();
		this.dbconf = new MRConfiguration(job);
		this.jobId = this.dbconf.getSysJobId();
		this.sysJobId = this.dbconf.getConf().get(JobContext.ID);// "mapred.job.id"
		this.taskId = this.dbconf.getConf().get(JobContext.TASK_ID); // 获取任务id
																		// "mapred.tip.id"
		this.jobLogId = this.dbconf.getJobLogId(); // 获取job的日志id
		this.perInputNumber = this.dbconf.getJOBLogMapInputPerRecordNumber(); // 实时记录输入数据量
		this.perOutputNumber = this.dbconf.getJOBLogMapOutputPerRecordNumber(); // 实时记录输入数据量

		this.inputMapEnd = this.dbconf.getInputMapEnd();// 是否map直接输出结果
		this.srcFieldNames = this.dbconf.getInputFieldNames();// 输入字段
		String[] targetFieldNames = this.dbconf.getOutputFieldNames();// 输出字段
		this.groupFieldMethod = this.dbconf.getRelatedGroupFieldMethod();// 目标到源映射及方法
		this.onlyFilterNotStorage = this.dbconf.getInputMapOnlyFilterNotStorage();// 只过滤，不入库的标示
		this.filterIllegalDataWriteFiles = this.dbconf.getInputMapFilterIllegalDatawriteFiles();// 数据过滤掉的数据写文件
		this.outColumnDefaultValue = StringUtil.decodeOutColumnDefaultValue(this.dbconf.getOutputColumnDefaultValue());// 获取直接输出的输出字段的默认值
		HiveConfiguration hconf = new HiveConfiguration(job);
		this.partitionField = hconf.getOutputHivePartitionField(); // hive分区字段

		this.debugRowNum = this.dbconf.getJobLogDebugRowNum();
		this.debug = this.dbconf.getJobLogDebug();

		// 若未指定输出字段，直接将输入字段作为输出字段
		if (targetFieldNames == null) {
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

	// @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// // NOTHING
		// }
		// public void close() throws IOException {
		super.cleanup(context);
		MRLog.consoleDebug(LOG, "Total: Map read " + this.recordCount + " rows, write" + this.outCount + " rows.");

		// map已经实际运行过
		if (this.isRun) {
			// 日志
			String msg = this.runflag == 1 ? "sucess" : "failed";
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

			String msg = "mapper monitor task nodePath is:" + zkTaskPath;
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
