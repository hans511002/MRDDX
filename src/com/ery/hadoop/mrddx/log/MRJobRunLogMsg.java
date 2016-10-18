package com.ery.hadoop.mrddx.log;

import java.util.Date;

/**
 * 系统日志信息(包括调试信息等)
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-20
 * @version v1.0
 */
public class MRJobRunLogMsg {
	private Date logDate; // 日志产生的时间(LOG_DATE)
	private long logId; // 日志ID（LOG_ID）
	private int logType; // 日志类型(LOG_TYPE[1:debug,2:info,3:warn:4:error,5:exeception])
	private String logMSG; // 日志信息 (LOG_MSG)

	/**
	 * 构造方法
	 * 
	 * @param date
	 *            日志产生的时间
	 * @param logId
	 *            日志ID
	 * @param type
	 *            日志类型
	 * @param logMsg
	 *            日志信息
	 */
	public MRJobRunLogMsg(Date date, long logId, int type, String logMsg) {
		this.logDate = date;
		this.logId = logId;
		this.logType = type;
		this.logMSG = logMsg;
	}

	public Date getLogDate() {
		return logDate;
	}

	public void setLogDate(Date logDate) {
		this.logDate = logDate;
	}

	public long getLogId() {
		return logId;
	}

	public void setLogId(long logId) {
		this.logId = logId;
	}

	public int getLogType() {
		return logType;
	}

	public void setLogType(int logType) {
		this.logType = logType;
	}

	public String getLogMSG() {
		return logMSG;
	}

	public void setLogMSG(String logMSG) {
		this.logMSG = logMSG;
	}
}
