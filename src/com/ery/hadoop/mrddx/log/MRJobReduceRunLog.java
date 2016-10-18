package com.ery.hadoop.mrddx.log;

import java.util.Date;

import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * MR_JOB_REDUCE_RUN_LOG表对应的javabean
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-4
 * @version v1.0
 */
public class MRJobReduceRunLog {
	private String reduceTaskId; // reduce任务Id(REDUCE_TASK_ID)
	private long logId; // 日志id与MR_JOB_RUN_LOG中LOG_ID关联(LOG_ID)
	private long reduceInputCount; // reduce输入数据量(MAP_INPUT_COUNT)
	private long reduceOutputCount; // reduce输出数据量(MAP_OUTPUT_COUNT)
	private Date startDate; // reduce开始时间(START_DATE)
	private Date endDate; // reduce结束时间(END_DATE)
	private int runFlag; // 结果标识符,取值范围[1-2]:1-成功,2-失败(RUN_FLAG)
	private String logMSG; // 日志消息(LOG_MSG)
	private long recordInvalidCount;// 过滤掉的记录数

	/**
	 * 默认构造函数
	 */
	public MRJobReduceRunLog() {
	}

	/**
	 * 构造函数
	 * 
	 * @param reduceTaskId
	 *            reduce任务Id
	 * @param logId
	 *            日志id
	 * @param reduceInputCount
	 *            reduce输入数据量
	 * @param reduceOutputCount
	 *            reduce输出数据量
	 * @param startDate
	 *            reduce开始时间
	 * @param endDate
	 *            reduce结束时间
	 * @param runFlag
	 *            结果标识符
	 * @param logMSG
	 *            日志消息
	 */
	public MRJobReduceRunLog(String reduceTaskId, long logId, long reduceInputCount, long reduceOutputCount, Date startDate, Date endDate,
			int runFlag, String logMSG, long recordInvalidCount) {
		this.reduceTaskId = reduceTaskId;
		this.logId = logId;
		this.reduceInputCount = reduceInputCount;
		this.reduceOutputCount = reduceOutputCount;
		this.startDate = startDate;
		this.endDate = endDate;
		this.runFlag = runFlag;
		this.logMSG = logMSG;
		this.recordInvalidCount = recordInvalidCount;
	}

	public String getReduceTaskId() {
		return reduceTaskId;
	}

	public void setReduceTaskId(String reduceTaskId) {
		this.reduceTaskId = reduceTaskId;
	}

	public long getLogId() {
		return logId;
	}

	public void setLogId(long logId) {
		this.logId = logId;
	}

	public long getReduceInputCount() {
		return reduceInputCount;
	}

	public void setReduceInputCount(long reduceInputCount) {
		this.reduceInputCount = reduceInputCount;
	}

	public long getReduceOutputCount() {
		return reduceOutputCount;
	}

	public void setReduceOutputCount(long reduceOutputCount) {
		this.reduceOutputCount = reduceOutputCount;
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
		str.append("reduceTaskId=>");
		str.append(this.reduceTaskId);
		str.append(",logId=>");
		str.append(this.logId);
		str.append(",reduceInputCount=>");
		str.append(this.reduceInputCount);
		str.append(",reduceOutputCount=>");
		str.append(this.reduceOutputCount);
		str.append(",startDate=>");
		str.append(StringUtil.dateToString(this.startDate, StringUtil.DATE_FORMAT_TYPE1));
		str.append(",endDate=>");
		str.append(StringUtil.dateToString(this.endDate, StringUtil.DATE_FORMAT_TYPE1));
		str.append(",runFlag=>");
		str.append(this.runFlag);
		str.append(",logMSG=>");
		str.append(this.logMSG);		
		str.append(",filterCount=>");
		str.append(this.logMSG);
		return str.toString();
	}
}
