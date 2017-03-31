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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.remote.plugin.IRemotePlugin;
import com.ery.hadoop.mrddx.senior.IMRSeniorApply;
import com.ery.hadoop.mrddx.senior.MRSeniorUtil;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.zk.IMonitorZKNode;
import com.ery.hadoop.mrddx.zk.MRWatcher;
import com.ery.hadoop.mrddx.zk.MonitorZKNode;

/**


 * 

 * @version v1.0
 * @create Data 2013-1-9
 */
public class DBReducer extends Reducer<DBRecord, DBRecord, DBRecord, NullWritable> implements IDBMonitor, IDBSenior {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(DBReducer.class);
	public static final MRLog MRLOG = MRLog.getInstance();
	public int debug = -1; // 日志级别 1：打印job配置参数,
	// 2:打印job解析后的参数(包含1)，3：打印map和redue的输入输出记录
	public int debugRowNum = -1; // 每个map输入条数限制(设置-debug参数时，才生效, 默认值：1000)

	/**
	 * 以下为日志参数
	 */
	// reduce执行是否成功, 取值范围[1-2]:1-成功,2-失败
	private int runflag = 1;

	// job的日志id号
	private long jobLogId;

	// 当前reduce是否实际运行过(true-运行过,false-未运行过)
	private boolean isRun;

	// 输出的记录数
	private long outCount;

	// 输入的记录数
	private long recordCount;

	// 无效的记录数
	private long recordInvalidCount;

	// 实时记录输入数据量
	private long perInputNumber;

	// 实时记录输入数据量
	private long perOutputNumber;

	/**
	 * 以下为业务参数
	 */
	// 监听节点对象
	private IMonitorZKNode monitorZKNode;

	// 是否需要监控
	private boolean isNeedZKMonitor;

	// Configuration
	private Configuration job;

	// MRConfiguration
	private MRConfiguration mrConf;

	// 任务ID
	private String taskId;

	// 是否map直接输出结果（true：直接输出）
	private boolean inputMapEnd;

	// 源字段的统计方法
	private String[] groupFieldMethod = null;

	// 目标字段
	private String[] destFieldNames = null;

	// null writable
	private NullWritable n = NullWritable.get();

	// 高级应用对象
	private IMRSeniorApply mrSeniorApply[];

	// 直接输出的输出字段默认值
	Map<String, String> outColumnDefaultValue;

	// 采集插件对象
	private IRemotePlugin remotePlugin;

	// 临时存放待过滤的key, value合并的值
	private DBRecord tempMapValue = new DBRecord();

	@Override
	protected void reduce(DBRecord key, Iterable<DBRecord> values, Context context) throws IOException,
			InterruptedException {

		// public void reduce(DBRecord key, Iterator<DBRecord> values,
		// OutputCollector<DBRecord, NullWritable> output,
		// Reporter reporter) throws IOException {
		// 监听任务节点
		if (this.isNeedZKMonitor && !this.inputMapEnd && null != this.monitorZKNode) {
			this.monitorZKNode.monitorTask();
		}

		// 实际运行标识
		if (!this.isRun) {
			// 日志
			if (MRLOG.queryJOBMapRunLogByReduceTaskId(this.taskId) <= 0) {
				MRLOG.addJobReduceRun(this.taskId, this.jobLogId, this.recordCount, this.outCount, new Date(),
						new Date(), this.runflag, "start");
			}
			this.isRun = true;
		}

		this.recordCount++;

		// 日志
		if (this.recordCount % this.perInputNumber == 0) {
			String msg = "input:" + this.recordCount + " row.";
			MRLog.debug(LOG, msg);
			MRLOG.jobReduceRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
		}

		try {
			// 处理方法（统计函数）对应的值.
			DBRecord sum = new DBRecord();
			Object sumObjField = null;
			boolean exitsCombiner = context.getCombinerClass() != null;// 判断在map是否设置过合并操作
			int rowCount = 0;

			// 打印日志记录
			this.debugInfo("输入:", key, null, 1);
			for (DBRecord _nrow : values) {
				if (_nrow.isNull()) {
					continue;
				}
				// 插件处理
				this.tempMapValue.clear();
				this.tempMapValue.getRow().putAll(_nrow.getRow());
				this.tempMapValue.getRow().putAll(key.getRow());
				List<Map<String, Object>> listVal = IRemotePlugin.mapPlugin(this.remotePlugin, tempMapValue.getRow());
				if (listVal == null || listVal.size() == 0) {
					this.recordInvalidCount++;
					debugInfo("该记录为非法");
					continue;
				}
				for (int x = 0; x < listVal.size(); x++) {
					DBRecord dbrecord = new DBRecord(listVal.get(x));
					dbrecord.setStatus(_nrow.isStatus());

					// 高级应用处理
					if (this.mrSeniorApply != null) {
						for (int j = 0; j < this.mrSeniorApply.length; j++) {
							if (null != this.mrSeniorApply[j]) {
								this.mrSeniorApply[j].apply(dbrecord, null);
							}
						}
					}

					// 无效
					if (!dbrecord.isStatus()) {
						continue;
					}

					// 汇总的行数
					if (exitsCombiner) {
						rowCount = StringUtil.objectToInt(dbrecord.getData(DBGroupReducer.GROUPBY_COUNTKEY)) +
								StringUtil.objectToInt(sum.getData(DBGroupReducer.GROUPBY_COUNTKEY));
						sum.putData(DBGroupReducer.GROUPBY_COUNTKEY, rowCount);
					} else {
						rowCount++;
					}

					// 处理统计函数
					for (int i = 0; i < this.destFieldNames.length; i++) {
						Integer method = DBGroupReducer.GROUP_METHODS.get(this.groupFieldMethod[i]);
						String fieldName = this.destFieldNames[i];
						if (method == 0) {
							continue;
						}

						sumObjField = sum.getData(fieldName);
						Object v = dbrecord.getData(fieldName);
						switch (method) {
						case 1: // SUM
						case 2: // AVG
							Double d = null == v ? 0 : StringUtil.stringToDouble(v.toString());
							Double objv = null == sumObjField ? 0 : StringUtil.stringToDouble(sumObjField.toString());
							sum.putData(fieldName, d + objv);
							break;
						case 3: // MIN
							sum.putData(fieldName, StringUtil.getSmallObject(v, sumObjField));
							break;
						case 4: // MAX
							sum.putData(fieldName, StringUtil.getBigObject(v, sumObjField));
							break;
						case 5: // COUNT
							if (exitsCombiner) {
								// 在map中已经合并过后，fileName的值应该是相加
								Object obj = sum.getData(fieldName);
								sum.putData(fieldName, null == obj ? Integer.parseInt(v.toString()) : ((Integer) obj) +
										Integer.parseInt(v.toString()));
							} else {
								// 没有在map中合并的值，fileName的值是递增相加
								Object obj = sum.getData(fieldName);
								sum.putData(fieldName, null == obj ? 1 : ((Integer) obj) + 1);
							}
							break;
						}
					}
				}
			}
			// 处理AVG
			for (int i = 0; i < this.destFieldNames.length; i++) {
				Integer method = DBGroupReducer.GROUP_METHODS.get(groupFieldMethod[i]);
				String fieldName = this.destFieldNames[i];
				if (method != 2) {
					continue;
				}

				sumObjField = sum.getData(fieldName);
				if (null != sumObjField) {
					double rsum = StringUtil.stringToDouble(sumObjField);
					if (rowCount > 0) {
						sum.putData(fieldName,
								StringUtil.doubleToString(rsum / rowCount, StringUtil.DOUBLE_FORMAT_8Decimal));
					}
				}
			}

			sum.removeData(DBGroupReducer.GROUPBY_COUNTKEY);
			key.getRow().putAll(this.outColumnDefaultValue);// 优先设置默认值（输出的默认值）
			key.mergerRecord(sum);
			IRemotePlugin.beforConvertRow(this.remotePlugin, key);
			context.write(key, this.n);
			this.outCount++;

			// 日志
			if (this.outCount % this.perOutputNumber == 0) {
				String msg = "output:" + this.recordCount + " row.";
				MRLog.debug(LOG, msg);
				MRLOG.jobReduceRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
			}

			// 打印日志记录
			this.debugInfo("输出:", key, null, 1);
		} catch (Exception e) {
			// 异常设置任务节点失败
			if (this.isNeedZKMonitor && !this.inputMapEnd && null != this.monitorZKNode) {
				try {
					this.monitorZKNode.setZKNodeData(MRWatcher.FAILED.getBytes());
				} catch (Exception e1) {
					String msg = "set zookeeper taskNode FAILED value exception!";
					MRLog.errorException(LOG, msg, e1);
					MRLOG.jobReduceRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg + " exception:" +
							StringUtil.stringifyException(e));
				}
			}

			// 设置运行异常标识符
			this.runflag = 2;

			// 输出日志信息
			String msg = "exception:" + StringUtil.stringifyException(e);
			MRLog.errorException(LOG, msg, e);
			MRLOG.jobReduceRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);

			throw new IOException(e);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// public void configure(JobConf job) {
		this.job = context.getConfiguration();
		this.mrConf = new MRConfiguration(job);
		this.taskId = this.mrConf.getConf().get("mapred.tip.id"); // 获取任务id
		this.jobLogId = this.mrConf.getJobLogId(); // 获取job的日志id
		this.perInputNumber = this.mrConf.getJOBLogReduceInputPerRecordNumber(); // 实时记录输入数据量
		this.perOutputNumber = this.mrConf.getJOBLogReduceOutputPerRecordNumber(); // 实时记录输入数据量

		this.inputMapEnd = this.mrConf.getInputMapEnd();// 是否map直接输出结果
		String[] srcFieldNames = this.mrConf.getInputFieldNames();// 输入字段
		String[] targetFieldNames = this.mrConf.getOutputFieldNames();// 输出字段
		this.groupFieldMethod = this.mrConf.getRelatedGroupFieldMethod();// 目标到源映射及方法
		this.outColumnDefaultValue = StringUtil.decodeOutColumnDefaultValue(this.mrConf.getOutputColumnDefaultValue());// 获取直接输出的输出字段的默认值

		this.debugRowNum = this.mrConf.getJobLogDebugRowNum(); // 打印日志的条数
		this.debug = this.mrConf.getJobLogDebug(); // 打印日志的级别

		if (targetFieldNames == null) {
			targetFieldNames = srcFieldNames.clone();
		}

		// 打印级别1
		if (this.debug >= MRConfiguration.INTERNAL_JOB_LOG_DEBUG_CONF_INFO) {
			StringBuffer orginConf = new StringBuffer();
			orginConf.append("\n实时记录输入数据量:" + this.perInputNumber + "\n");
			orginConf.append("实时记录输入数据量:" + this.perOutputNumber + "\n");
			orginConf.append("输入字段:" + StringUtil.toString(srcFieldNames, null) + "\n");
			orginConf.append("输出字段:" + StringUtil.toString(targetFieldNames, null) + "\n");
			orginConf.append("目标到源映射及方法:" + StringUtil.toString(this.groupFieldMethod, null) + "\n");
			MRLog.consoleDebug(LOG, orginConf.toString());
		}

		Set<String> setSrcFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setSrcFieldNames, srcFieldNames);

		Set<String> setTargetFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setTargetFieldNames, targetFieldNames);

		if (!this.inputMapEnd) {
			analyzeConfig(job, setSrcFieldNames, setTargetFieldNames);
		}

		// 初始化日志信息
		MRLog.getInstance().setConf(this.mrConf);

		// 设置zk监听参数
		this.isNeedZKMonitor = this.mrConf.isZKMonitor();
		this.monitor(this.mrConf);

		// 初始化高级应用对象
		this.senior(this.mrConf);

		// 初始化插件
		this.remotePlugin = IRemotePlugin.configurePlugin(context, this);

		// 打印级别2
		if (this.debug >= MRConfiguration.INTERNAL_JOB_LOG_DEBUG_DECONF_INFO) {
			StringBuffer deConf = new StringBuffer();
			deConf.append("\n任务id:" + this.taskId + "\n");
			deConf.append("JOB的日志ID:" + this.jobLogId + "\n");
			deConf.append("是否map直接输出结果:" + this.inputMapEnd + "\n");
			deConf.append("处理后的输入字段:" + StringUtil.toString(this.destFieldNames, null) + "\n");
			deConf.append("处理后的目标到源映射及方法:" + StringUtil.toString(this.groupFieldMethod, null) + "\n");
			MRLog.consoleDebug(LOG, deConf.toString());
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
		this.destFieldNames = new String[groupFieldMethod.length];
		for (int i = 0; i < groupFieldMethod.length; i++) {
			String[] tmp = groupFieldMethod[i].split(":");
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
			this.groupFieldMethod[i] = tmp[2];
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// public void close() throws IOException {
		super.cleanup(context);
		MRLog.consoleDebug(LOG, "Total: reduce read " + this.recordCount + " rows, write " + this.outCount + " rows.");

		// map已经实际运行过
		if (this.isRun) {
			// 日志
			String msg = this.runflag == 1 ? "sucess" : "failed";
			MRLOG.updateJobReduceRun(this.taskId, this.jobLogId, this.recordCount, this.outCount, null, new Date(),
					this.runflag, msg, this.recordInvalidCount);

			// 添加到记录数
			MRLOG.updateReduceCountJobRun(this.jobLogId, this.recordCount, this.outCount, this.recordInvalidCount);
		}
		IRemotePlugin.closePlugin(this.remotePlugin);
	}

	@Override
	public void monitor(MRConfiguration conf) {
		String zkTaskPath = null;
		if (!this.inputMapEnd && this.isNeedZKMonitor) {
			String zkAddress = conf.getZKAddress();
			String zkRootPath = conf.getZKReducePath();
			zkTaskPath = zkRootPath + "/" + taskId;
			this.monitorZKNode = new MonitorZKNode(taskId, zkTaskPath, zkAddress);

			String msg = "reduce monitor task nodePath is:" + zkTaskPath;
			MRLog.infoZK(LOG, msg);
			MRLOG.jobReduceRunMsg(this.taskId, MRLog.LOG_TYPE_INFO, new Date(), msg);
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

		this.mrSeniorApply = MRSeniorUtil.getMRSeniorApplies(sconf, conf.getConf(), 1);
	}

	/**
	 * 打印记录的日志信息
	 * 
	 * @param value
	 *            日志信息
	 * @param nrow
	 * @param key
	 * @param inOrOut
	 *            0:输入, 1:输出
	 */
	private void debugInfo(String prefix, DBRecord key, DBRecord nrow, int inOrOut) {
		// 打印级别3
		if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD) {
			if (this.debugRowNum >= this.recordCount) {
				String k = null == key ? "" : key.toString();
				String v = null == nrow ? "" : nrow.toString();
				if (inOrOut == 0) {
					MRLog.consoleDebug(LOG, prefix + "key=>" + k + " value=>" + v);
				} else {
					MRLog.consoleDebug(LOG, prefix + k);
				}
			}
		}
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
