package com.ery.hadoop.mrddx.log;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.mode.MRFileListPO;
import com.ery.hadoop.mrddx.remote.FTPLogPO;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * 日志工具类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-4
 * @version v1.0
 */
public class MRLog {
	// (maxlength:记录信息的最大长度;type:记录日志的类型,多个使用逗号(,)隔开)
	public static final String NTERNAL_JOB_LOG_RUN_MSG_MAXLENGTH = "sys.log.conf.job.run.msg.maxlength";
	public static final String NTERNAL_JOB_LOG_RUN_MSG_TYPE = "sys.log.conf.job.run.msg.type";
	// 日志的连接参数
	public static final String JOB_SYS_LOG_JDBC_URL = "sys.log.conf.jdbc.url";
	public static final String JOB_SYS_LOG_JDBC_USER = "sys.log.conf.jdbc.username";
	public static final String JOB_SYS_LOG_JDBC_PWD = "sys.log.conf.jdbc.password";

	/**
	 * MR日志实例
	 */
	private static final MRLog INSTANCE = new MRLog();

	// 日志类型
	public static int LOG_TYPE_DEBUG = 1;
	public static int LOG_TYPE_INFO = 2;
	public static int LOG_TYPE_WARN = 3;
	public static int LOG_TYPE_ERROR = 4;

	// 日志前缀
	public static final String LOG_PREFIX = "[MR-LOG]";
	// zookeeper日志前缀
	public static final String LOG_ZK_PREFIX = "[MR-ZK-LOG]";
	// 控制台输出日志前缀debug
	public static final String LOG_PREFIX_DEBUG = "[MR-LOG-debug]";
	// 控制台输出日志前缀info
	public static final String LOG_PREFIX_INFO = "[MR-LOG-info]";
	// 控制台输出日志前缀warn
	public static final String LOG_PREFIX_WARN = "[MR-LOG-warn]";
	// 控制台输出日志前缀error
	public static final String LOG_PREFIX_ERROR = "[MR-LOG-error]";

	// 数据库的日志输出对象
	private DBLogOutput dblog;

	// 系统job的日志Id号
	private long logId;
	// 是否是测试任务.
	private boolean isTestJob;
	private int msgMaxLength;

	private boolean isDebug;
	private boolean isInfo;
	private boolean isWarn;
	private boolean isError;
	private boolean isException;

	/**
	 * 私有构造方法
	 */
	private MRLog() {
		init();
	}

	/**
	 * 输出日志到数据库，必须初始化
	 * 
	 * @param dbconf
	 */
	public void init() {
		// 获取properties配置
		Properties prop = new Properties();
		InputStream in = getClass().getResourceAsStream("/mrddx.properties");
		try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}

		String[] types = new String[0];
		String url = null;
		String user = null;
		String pwd = null;
		Set<?> keyValue = prop.keySet();
		for (Iterator<?> it = keyValue.iterator(); it.hasNext();) {
			String key = (String) it.next();
			if (NTERNAL_JOB_LOG_RUN_MSG_MAXLENGTH.equals(key)) {
				String value = (String) prop.get(key);
				int length = StringUtil.stringToInt(value, 0);
				this.msgMaxLength = length < 100 ? 100 : length;
			}

			if (NTERNAL_JOB_LOG_RUN_MSG_TYPE.equals(key)) {
				String value = (String) prop.get(key);
				if (null == value) {
					types = new String[0];
				}

				types = value.split(",");
			}

			if (JOB_SYS_LOG_JDBC_URL.equals(key)) {
				url = (String) prop.get(key);
			}

			if (JOB_SYS_LOG_JDBC_USER.equals(key)) {
				user = (String) prop.get(key);
			}

			if (JOB_SYS_LOG_JDBC_PWD.equals(key)) {
				pwd = (String) prop.get(key);
			}
		}

		for (String t : types) {
			int tmp = StringUtil.stringToInt(t, -1);
			switch (tmp) {
			case 1:
				this.isDebug = true;
				break;
			case 2:
				this.isInfo = true;
				break;
			case 3:
				this.isWarn = true;
				break;
			case 4:
				this.isError = true;
				break;
			case 5:
				this.isException = true;
				break;
			default:
				break;
			}
		}

		this.dblog = new DBLogOutput(url, user, pwd);
	}

	public static MRLog getInstance() {
		return INSTANCE;
	}

	/**
	 * 输出整个JOB的统计信息
	 */
	public void addJobRun(long logId, long jobId, String monthNo, String dataNo, Date startDate, int allFileSize,
			String execCMD, String queue) {
		if (this.isTestJob) {
			return;
		}
		MRJobRunLog log = new MRJobRunLog(logId, jobId, monthNo, dataNo, startDate, null, 0, 0, allFileSize, execCMD,
				null);
		log.setQueueName(queue);
		this.dblog.output(log);
	}

	/**
	 * 输出整个JOB的统计信息
	 */
	public void updateJobRun(long logId, Date endDate, int runFlag, long rowRecord, String logMSG) {
		if (this.isTestJob) {
			return;
		}
		MRJobRunLog log = new MRJobRunLog(logId, 0, null, null, null, endDate, runFlag, rowRecord, 0, null, logMSG);
		this.dblog.outputUpdate(log);
	}

	/**
	 * 输出整个JOB日志信息
	 */
	public void addJobRunMsg(Date logDate, int logType, String logMsg) {
		if (this.isTestJob) {
			return;
		}

		if (null == logMsg) {
			logMsg = "";
		}

		if (logMsg.length() > this.msgMaxLength) {
			logMsg = logMsg.substring(0, this.msgMaxLength);
		}

		MRJobRunLogMsg log = new MRJobRunLogMsg(logDate, this.logId, logType, logMsg);
		this.dblog.outputLogMsg(log);
	}

	/**
	 * 输出整个JOB日志信息
	 */
	public void addJobRunMsg(long logId, Date logDate, int logType, String logMsg) {
		if (this.isTestJob) {
			return;
		}

		if (null == logMsg) {
			logMsg = "";
		}

		if (logMsg.length() > this.msgMaxLength) {
			logMsg = logMsg.substring(0, this.msgMaxLength);
		}

		MRJobRunLogMsg log = new MRJobRunLogMsg(logDate, logId, logType, logMsg);
		this.dblog.outputLogMsg(log);
	}

	/**
	 * 添加Map记录数到整个JOB的统计信息中
	 */
	public void updateMapCountJobRun(long logId, long inputCount, long outputCount, long recordInvalidCount) {
		if (this.isTestJob) {
			return;
		}
		this.dblog.outputMapCountUpdate(logId, inputCount, outputCount, recordInvalidCount);
	}

	/**
	 * 添加Reduce记录数到整个JOB的统计信息中
	 */
	public void updateReduceCountJobRun(long logId, long inputCount, long outputCount, long recordInvalidCount) {
		if (this.isTestJob) {
			return;
		}
		this.dblog.outputReduceCountUpdate(logId, inputCount, outputCount, recordInvalidCount);
	}

	/**
	 * 添加文件总大小
	 * 
	 * @param jobLogId
	 * @param totalSize
	 */
	public void updateFileSizeJobRun(long jobLogId, long totalSize) {
		if (this.isTestJob) {
			return;
		}
		this.dblog.outputFileSizeUpdate(jobLogId, totalSize);
	}

	/**
	 * 输出MAP的统计信息
	 */
	public void addJobMapRun(String mapTaskId, long logId, long mapInputCount, long mapOutputCount, Date startDate,
			Date endDate, int runFlag, String logMSG) {
		if (this.isTestJob) {
			return;
		}
		MRJobMapRunLog log = new MRJobMapRunLog(mapTaskId, logId, mapInputCount, mapOutputCount, startDate, endDate,
				runFlag, logMSG, 0);
		this.dblog.output(log);
	}

	/**
	 * 输出MAP的统计信息
	 */
	public int queryJOBMapRunLogByMapTaskId(String mapTaskId) {
		if (this.isTestJob) {
			return 0;
		}
		return this.dblog.queryJOBMapRunLogByMapTaskId(mapTaskId);
	}

	/**
	 * 输出reduce的统计信息
	 */
	public int queryJOBMapRunLogByReduceTaskId(String mapTaskId) {
		if (this.isTestJob) {
			return 0;
		}
		return this.dblog.queryJOBMapRunLogByReduceTaskId(mapTaskId);
	}

	/**
	 * 更新MAP的统计信息
	 */
	public void updateJobMapRun(String mapTaskId, long logId, long mapInputCount, long mapOutputCount, Date startDate,
			Date endDate, int runFlag, String logMSG, long recordInvalidCount) {
		if (this.isTestJob) {
			return;
		}
		MRJobMapRunLog log = new MRJobMapRunLog(mapTaskId, logId, mapInputCount, mapOutputCount, startDate, endDate,
				runFlag, logMSG, recordInvalidCount);
		this.dblog.outputUpdate(log);
	}

	/**
	 * 输出REDUCE的统计信息
	 */
	public void addJobReduceRun(String reduceTaskId, long logId, long reduceInputCount, long reduceOutputCount,
			Date startDate, Date endDate, int runFlag, String logMSG) {
		if (this.isTestJob) {
			return;
		}
		MRJobReduceRunLog log = new MRJobReduceRunLog(reduceTaskId, logId, reduceInputCount, reduceOutputCount,
				startDate, endDate, runFlag, logMSG, 0);
		this.dblog.output(log);
	}

	/**
	 * 更新REDUCE的统计信息
	 */
	public void updateJobReduceRun(String reduceTaskId, long logId, long reduceInputCount, long reduceOutputCount,
			Date startDate, Date endDate, int runFlag, String logMSG, long recordInvalidCount) {
		if (this.isTestJob) {
			return;
		}
		MRJobReduceRunLog log = new MRJobReduceRunLog(reduceTaskId, logId, reduceInputCount, reduceOutputCount,
				startDate, endDate, runFlag, logMSG, recordInvalidCount);
		this.dblog.outputUpdate(log);
	}

	/**
	 * 输出MAP运行过程中的日志信息
	 */
	public void jobMapRunMsg(String taskId, int logType, Date LogDate, String logMsg) {
		if (this.isTestJob) {
			return;
		}
		MRJobLogMsg log = new MRJobLogMsg(taskId, logType, LogDate, logMsg);
		this.dblog.outputMapLogMsg(log);
	}

	/**
	 * 输出MAP运行过程中的日志信息
	 */
	public void jobMapRunMsg(String taskId, int logType, Date LogDate, String logMsg, Throwable e) {
		if (this.isTestJob) {
			return;
		}

		String msg = StringUtil.stringifyException(e);
		MRJobLogMsg log = new MRJobLogMsg(taskId, logType, LogDate, logMsg + ":" + msg);
		this.dblog.outputMapLogMsg(log);
	}

	/**
	 * 输出REDUCE运行过程中的日志信息
	 */
	public void jobReduceRunMsg(String taskId, int logType, Date LogDate, String logMsg) {
		if (this.isTestJob) {
			return;
		}
		MRJobLogMsg log = new MRJobLogMsg(taskId, logType, LogDate, logMsg);
		this.dblog.outputReudceLogMsg(log);
	}

	/**
	 * 输出整个JOB与远程文件的日志信息
	 */
	public void addJobRemoteFileMsg(FTPLogPO ftpLogPO) {
		this.dblog.outputRemoteFileMsg(ftpLogPO);
	}

	/**
	 * 输出整个JOB与远程文件的日志信息
	 */
	public void addJobRemoteFileMsgBatch(List<FTPLogPO> ftpLogPO) {
		this.dblog.outputRemoteFileMsgBatch(ftpLogPO);
	}

	/**
	 * 输出整个JOB与远程文件的日志信息
	 * 
	 * @param outputFileInfo
	 */
	public void updateJobRemoteFileMsg(FTPLogPO ftpLogPO) {
		this.dblog.outputUpdateRemoteFileMsg(ftpLogPO);
	}

	/**
	 * 输出整个JOB与远程文件的日志信息
	 * 
	 * @param outputFileInfo
	 */
	public void updateJobRemoteFileMsgBatch(List<FTPLogPO> ftpLogPO) {
		this.dblog.outputUpdateRemoteFileMsgBatch(ftpLogPO);
	}

	/**
	 * 获取整个JOB与远程文件的日志信息
	 * 
	 * @param outputFileInfo
	 */
	public FTPLogPO getJobRemoteFileCreateDate(long colId, long colLogId, String inputFileInfo, String outputFileInfo) {
		if (null == inputFileInfo || inputFileInfo.trim().length() <= 0 || null == outputFileInfo ||
				outputFileInfo.trim().length() <= 0) {
			return null;
		}

		return this.dblog.getJobRemoteFileCreateDate(colId, colLogId, inputFileInfo, outputFileInfo);
	}

	/**
	 * 获取需要更新的日志信息
	 * 
	 * @param colId
	 * @param colLogId
	 * @param colLogId2
	 * @param i
	 * @param colFileDetailId
	 * @return
	 */
	public Object[] getJobMRColDetailLog(long colId, long colLogId, String inputFileInfo, String outputFileInfo) {
		if (null == inputFileInfo || inputFileInfo.trim().length() <= 0 || null == outputFileInfo ||
				outputFileInfo.trim().length() <= 0) {
			return null;
		}
		return this.dblog.getMRColDetailLogNeedUpdate(colId, colLogId, inputFileInfo, outputFileInfo);
	}

	/**
	 * 获取需要更新的日志信息
	 * 
	 * @param colId
	 * @param colLogId
	 * @param colLogId2
	 * @param i
	 * @param colFileDetailId
	 * @return
	 */
	public List<Object[]> getColIdFailList(long colId, long colLogId) {
		return this.dblog.getColFailList(colId, colLogId);
	}

	/**
	 * 获取采集任务的最近一次的日志ID.
	 * 
	 * @param colId
	 * @return
	 */
	public long getJobMRColLastLogId(long colId) {
		return this.dblog.getJobMRColLastLogId(colId);
	}

	/**
	 * 获取整个JOB与远程文件的日志信息
	 * 
	 * @param outputFileInfo
	 */
	public MRFileListPO getFilePoByFileId(String fileId) {
		if (null == fileId || fileId.trim().length() <= 0) {
			return null;
		}
		return this.dblog.getFilePoByFileId(fileId);
	}

	/**
	 * 获取整个JOB与远程文件的日志信息
	 * 
	 * @param outputFileInfo
	 */
	public boolean isExistJobRemoteFile(long jobId, String inputFileInfo, String outputFileInfo) {
		if (null == inputFileInfo || inputFileInfo.trim().length() <= 0 || null == outputFileInfo ||
				outputFileInfo.trim().length() <= 0) {
			return false;
		}

		return this.dblog.isExistJobRemoteFile(jobId, inputFileInfo, outputFileInfo);
	}

	public void colFileLog(MRColFileLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.colFileLog(log);
	}

	public long getColFileLogStatus(MRColFileLog log) {
		if (INSTANCE.isTestJob) {
			return -1;
		}

		return this.dblog.getColFileLogStatus(log);
	}

	public long[] queryColTaskLogResult(long colLogId) {
		if (INSTANCE.isTestJob) {
			return new long[2];
		}

		return this.dblog.queryColTaskLogResult(colLogId);
	}

	public void updateReRunColFileLog(MRColFileLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.updateReRunColFileLog(log);
	}

	public void updateColFileLog(MRColFileLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.updateColFileLog(log);
	}

	public long colFileDetailLog(MRColDetailLog log) {
		if (INSTANCE.isTestJob) {
			return -1l;
		}

		return this.dblog.colFileDetailLog(log);
	}

	public long getColFileDetailLog() {
		if (INSTANCE.isTestJob) {
			return -1l;
		}

		return this.dblog.getColFileDetailLog();
	}

	public int[] addColFileDetailLogBatch(List<MRColDetailLog> lstlog) {
		if (INSTANCE.isTestJob) {
			return new int[0];
		}

		return this.dblog.addColFileDetailLogBatch(lstlog);
	}

	public void updateColFileDetailLog(MRColDetailLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.updateColFileDetailLog(log);
	}

	public void colFileErrorLog(MRColFileErrorLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.colFileErrorLog(log);
	}

	public MRJobMapDataLog getMRJobMapDataLog(long jobId, String fileId) {
		if (INSTANCE.isTestJob) {
			return null;
		}

		return this.dblog.getMRJobMapDataLog(jobId, fileId);
	}

	public MRJobMapDataLog getMRJobMapDataLog(long jobId, String fileId, int jobLogType) {
		if (INSTANCE.isTestJob) {
			return null;
		}

		return this.dblog.getMRJobMapDataLog(jobId, fileId);
	}

	public List<MRJobMapDataLog> getMRJobMapDataLog(FileConfiguration fileconf, int jobLogType) {
		if (INSTANCE.isTestJob) {
			return null;
		}
		long jobLogId = fileconf.getInputFileJobLogId();
		long jobId = fileconf.getSysJobId();
		String timeRange[] = fileconf.getInputFileJobLogTime();
		switch (jobLogType) {
		case 2:
			return this.dblog.getMRJobMapDataLog(jobId, jobLogId);
		case 3:
			return this.dblog.getMRJobMapDataLog(jobId, timeRange);
		case 4:
			return this.dblog.getMRJobMapDataLog(jobId);
		default:
			break;
		}

		return new ArrayList<MRJobMapDataLog>();
	}

	public List<MRJobMapDataLog> getMRJobMapDataLog(long jobLogId, long jobId, String timeRange[], int jobLogType) {
		if (INSTANCE.isTestJob) {
			return null;
		}
		switch (jobLogType) {
		case 2:
			return this.dblog.getMRJobMapDataLog(jobId, jobLogId);
		case 3:
			return this.dblog.getMRJobMapDataLog(jobId, timeRange);
		case 4:
			return this.dblog.getMRJobMapDataLog(jobId);
		default:
			break;
		}

		return new ArrayList<MRJobMapDataLog>();
	}

	public int getMRJobMapDataLogSingleFile(long id, long jobLogId) {
		if (INSTANCE.isTestJob) {
			return -1;
		}
		return this.dblog.getMRJobMapDataLogSingleFile(id, jobLogId);
	}

	public void updateMRJobMapDataLogStatus(long id, int status) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.updateMRJobMapDataLogStatus(id, status);
	}

	public void addMRJobMapDataLog(MRJobMapDataLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.addMRJobMapDataLog(log);
	}

	public void addMRJobMapDataLogFTP(MRJobMapDataLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.addMRJobMapDataLogFTP(log);
	}

	public void setMRJobMapDataLogId(MRJobMapDataLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.setMRJobMapDataLogId(log);
	}

	public void addMRJobMapDataLogFTPBatch(List<MRJobMapDataLog> log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.addMRJobMapDataLogFTPBatch(log);
	}

	public void updateMRJobMapDataLog(MRJobMapDataLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.updateMRJobMapDataLog(log);
	}

	public void updateMRJobMapDataLogFTP(MRJobMapDataLog log) {
		if (INSTANCE.isTestJob) {
			return;
		}
		this.dblog.updateMRJobMapDataLogFTP(log);
	}

	public void addFileListLog(MRFileListPO file) {
		if (INSTANCE.isTestJob) {
			return;
		}

		this.dblog.addFileListLog(file);
	}

	public static void debug(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isDebug) {
			INSTANCE.addJobRunMsg(new Date(), 1, msg);
			log.info(LOG_PREFIX + msg);
		}
	}

	public static void info(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isInfo) {
			INSTANCE.addJobRunMsg(new Date(), 2, msg);
		}
		log.info(LOG_PREFIX + msg);
	}

	public static void info(Log log, long logId, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isInfo) {
			INSTANCE.addJobRunMsg(logId, new Date(), 2, msg);
		}
		log.info(LOG_PREFIX + msg);
	}

	public static void warn(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isWarn) {
			INSTANCE.addJobRunMsg(new Date(), 3, msg);
		}
		log.warn(LOG_PREFIX + msg);
	}

	public static void warn(Log log, long logId, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isWarn) {
			INSTANCE.addJobRunMsg(logId, new Date(), 3, msg);
		}
		log.warn(LOG_PREFIX + msg);
	}

	public static void error(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isError) {
			INSTANCE.addJobRunMsg(new Date(), 4, msg);
		}
		log.error(LOG_PREFIX + msg);
	}

	public static void fatal(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isError) {
			INSTANCE.addJobRunMsg(new Date(), 4, msg);
		}
		log.fatal(LOG_PREFIX + msg);
	}

	public static void warnException(Log log, Throwable e) {
		if (INSTANCE.isTestJob) {
			return;
		}
		String msg = StringUtil.stringifyException(e);
		if (INSTANCE.isWarn) {
			INSTANCE.addJobRunMsg(new Date(), 4, msg);
		}
		log.warn(LOG_PREFIX + msg);
	}

	public static void warnException(Log log, String msg, Throwable e) {
		if (INSTANCE.isTestJob) {
			return;
		}
		msg = msg + ":" + StringUtil.stringifyException(e);
		if (INSTANCE.isWarn) {
			INSTANCE.addJobRunMsg(new Date(), 4, msg);
		}
		log.warn(LOG_PREFIX + msg);
	}

	public static void errorException(Log log, Throwable e) {
		if (INSTANCE.isTestJob) {
			return;
		}
		String msg = StringUtil.stringifyException(e);
		if (INSTANCE.isException) {
			INSTANCE.addJobRunMsg(new Date(), 5, msg);
		}
		log.error(LOG_PREFIX + msg);
	}

	public static void errorException(Log log, String msg, Throwable e) {
		if (INSTANCE.isTestJob) {
			return;
		}
		msg = msg + ":" + StringUtil.stringifyException(e);
		if (INSTANCE.isException) {
			INSTANCE.addJobRunMsg(new Date(), 5, msg);
		}
		log.error(LOG_PREFIX + msg);
	}

	public static void consoleDebug(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isDebug) {
			INSTANCE.addJobRunMsg(new Date(), 1, msg);
		}
		System.out.println(LOG_PREFIX_DEBUG + msg);
	}

	public static void consoleInfo(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isInfo) {
			INSTANCE.addJobRunMsg(new Date(), 2, msg);
		}
		System.out.println(LOG_PREFIX_INFO + msg);
	}

	public static void consoleInfo(Log log, long logId, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isInfo) {
			INSTANCE.addJobRunMsg(logId, new Date(), 2, msg);
		}
		System.out.println(LOG_PREFIX_INFO + msg);
	}

	public static void consoleWarn(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isWarn) {
			INSTANCE.addJobRunMsg(new Date(), 3, msg);
		}
		System.out.println(LOG_PREFIX_WARN + msg);
	}

	public static void consoleError(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isError) {
			INSTANCE.addJobRunMsg(new Date(), 4, msg);
		}
		System.out.println(LOG_PREFIX_ERROR + msg);
	}

	public static void infoZK(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isInfo) {
			INSTANCE.addJobRunMsg(new Date(), 2, msg);
		}
		log.info(LOG_ZK_PREFIX + msg);
	}

	public static void errorZK(Log log, String msg) {
		if (INSTANCE.isTestJob) {
			return;
		}
		if (INSTANCE.isError) {
			INSTANCE.addJobRunMsg(new Date(), 4, msg);
		}
		log.error(LOG_ZK_PREFIX + msg);
	}

	public static void errorExceptionZK(Log log, String msg, Throwable e) {
		if (INSTANCE.isTestJob) {
			return;
		}
		msg = msg + ":" + StringUtil.stringifyException(e);
		if (INSTANCE.isException) {
			INSTANCE.addJobRunMsg(new Date(), 5, msg);
		}
		log.error(LOG_ZK_PREFIX + msg);
	}

	public static void systemOut(String msg) {
		System.out.println(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1) + " CONSOLE " + msg);
	}

	public static String get2Space() {
		return "    ";
	}

	public static String get4Space() {
		return "        ";
	}

	public static String get6Space() {
		return "            ";
	}

	public void setConf(MRConfiguration dbconf) {
		if (null == dbconf) {
			return;
		}

		this.logId = dbconf.getJobLogId();
		this.isTestJob = dbconf.isTestJob();
	}

	public boolean isTestJob() {
		return isTestJob;
	}

	/**
	 * 查询出详细日志信息
	 * 
	 * @param detailLogId
	 * @return
	 */
	public MRColDetailLog getDetailLog(long detailLogId) {

		return this.dblog.getDetailLog(detailLogId);
	}
}
