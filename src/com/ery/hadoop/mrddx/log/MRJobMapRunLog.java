package com.ery.hadoop.mrddx.log;

import java.util.Date;

import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * MR_JOB_MAP_RUN_LOG表对应的javabean
 * 



 * @createDate 2013-2-4
 * @version v1.0
 */
public class MRJobMapRunLog {
	private String mapTaskId; // Map任务Id(MAP_TASK_ID)
	private long logId; // 日志id与MR_JOB_RUN_LOG中LOG_ID关联(LOG_ID)
	private long mapInputCount; // Map输入数据量(MAP_INPUT_COUNT)
	private long mapOutputCount; // Map输出数据量(MAP_OUTPUT_COUNT)
	private Date startDate; // Map开始时间(START_DATE)
	private Date endDate; // Map结束时间(END_DATE)
	private int runFlag; // 结果标识符,取值范围[1-2]:1-成功,2-失败(RUN_FLAG)
	private String logMSG; // 日志消息(LOG_MSG)
	private long recordInvalidCount;// 过滤掉的记录数

	/**
	 * 默认构造方法
	 */
	public MRJobMapRunLog() {

	}

	/**
	 * 构造函数
	 * 
	 * @param mapTaskId
	 *            reduce任务Id
	 * @param logId
	 *            日志id
	 * @param mapInputCount
	 *            map输入数据量
	 * @param mapOutputCount
	 *            map输出数据量
	 * @param startDate
	 *            map开始时间
	 * @param endDate
	 *            map结束时间
	 * @param runFlag
	 *            结果标识符
	 * @param logMSG
	 *            日志消息
	 */
	public MRJobMapRunLog(String mapTaskId, long logId, long mapInputCount, long mapOutputCount, Date startDate, Date endDate, int runFlag,
			String logMSG, long recordInvalidCount) {
		this.mapTaskId = mapTaskId;
		this.logId = logId;
		this.mapInputCount = mapInputCount;
		this.mapOutputCount = mapOutputCount;
		this.startDate = startDate;
		this.endDate = endDate;
		this.runFlag = runFlag;
		this.logMSG = logMSG;
		this.recordInvalidCount = recordInvalidCount;
	}

	public String getMapTaskId() {
		return mapTaskId;
	}

	public void setMapTaskId(String mapTaskId) {
		this.mapTaskId = mapTaskId;
	}

	public long getLogId() {
		return logId;
	}

	public void setLogId(long logId) {
		this.logId = logId;
	}

	public long getMapInputCount() {
		return mapInputCount;
	}

	public void setMapInputCount(long mapInputCount) {
		this.mapInputCount = mapInputCount;
	}

	public long getMapOutputCount() {
		return mapOutputCount;
	}

	public void setMapOutputCount(long mapOutputCount) {
		this.mapOutputCount = mapOutputCount;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public int getRunFlag() {
		return runFlag;
	}

	public void setRunFlag(int runFlag) {
		this.runFlag = runFlag;
	}

	public String getLogMSG() {
		return logMSG;
	}

	public void setLogMSG(String logMSG) {
		this.logMSG = logMSG;
	}
	
	public long getRecordInvalidCount() {
		return recordInvalidCount;
	}

	public void setRecordInvalidCount(long recordInvalidCount) {
		this.recordInvalidCount = recordInvalidCount;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("info:");
		str.append("mapTaskId=>");
		str.append(this.mapTaskId);
		str.append(",logId=>");
		str.append(this.logId);
		str.append(",mapInputCount=>");
		str.append(this.mapInputCount);
		str.append(",mapOutputCount=>");
		str.append(this.mapOutputCount);
		str.append(",startDate=>");
		str.append(StringUtil.dateToString(this.startDate, StringUtil.DATE_FORMAT_TYPE1));
		str.append(",endDate=>");
		str.append(StringUtil.dateToString(this.endDate, StringUtil.DATE_FORMAT_TYPE1));
		str.append(",runFlag=>");
		str.append(this.runFlag);
		str.append(",logMSG=>");
		str.append(this.logMSG);
		str.append(",filterrow=>");
		str.append(this.recordInvalidCount);

		return str.toString();
	}
}
