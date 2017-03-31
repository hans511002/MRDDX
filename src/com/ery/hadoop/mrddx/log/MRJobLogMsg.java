package com.ery.hadoop.mrddx.log;

import java.util.Date;

import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * 日志信息
 * 



 * @createDate 2013-2-4
 * @version v1.0
 */
public class MRJobLogMsg {
	private String taskId;// 任务ID,与MRJobReduceRunLog的reduceTaskId或者MRJobMapRunLog的mapTaskId关联
	private int logType;// 日志类型,取值范围[1-4]:1-debug,2-info,3-warn,4-error
	private Date LogDate;// 产生日志时间
	private String logMsg;// 日志详情

	/**
	 * 默认构造方法
	 */
	public MRJobLogMsg() {
	}

	/**
	 * 构造方法
	 * 
	 * @param logId
	 *            日志ID
	 * @param taskId
	 *            任务ID
	 * @param logType
	 *            日志类型
	 * @param LogDate
	 *            产生日志时间
	 * @param logMsg
	 *            日志详情
	 */
	public MRJobLogMsg(String taskId, int logType, Date LogDate, String logMsg) {
		this.taskId = taskId;
		this.logType = logType;
		this.LogDate = LogDate;
		this.logMsg = logMsg;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public int getLogType() {
		return logType;
	}

	public void setLogType(int logType) {
		this.logType = logType;
	}

	public Date getLogDate() {
		return LogDate;
	}

	public void setLogDate(Date logDate) {
		LogDate = logDate;
	}

	public String getLogMsg() {
		return logMsg;
	}

	public void setLogMsg(String logMsg) {
		this.logMsg = logMsg;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("info:");
		str.append("taskId=>");
		str.append(this.taskId);
		str.append(",logType=>");
		str.append(this.logType);
		str.append(",LogDate=>");
		str.append(StringUtil.dateToString(this.LogDate, StringUtil.DATE_FORMAT_TYPE1));
		str.append(",logMsg=>");
		str.append(this.logMsg);
		return str.toString();
	}
}
