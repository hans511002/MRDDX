package com.ery.hadoop.mrddx.log;

import java.util.Date;

import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * MR_RUN_LOG表对应的javabean
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-4
 * @version v1.0
 */
public class MRJobRunLog {
	private long logId; // 日志ID（LOG_ID）
	private long jobId; // JOB ID号(JOB_ID)
	private String monthNo; // 执行的月份,格式:YYYYMM(MONTH_NO)
	private String dataNo; // 参数的日期,格式:YYYYMMDD(DATA_NO)
	private Date startDate; // 开始时间(START_DATE)
	private Date endDate; // 结束时间(END_DATE)
	private int runFlag; // 运行状态,取值范围[1-2]:1-成功,2-失败(RUN_FLAG)
	private long rowRecord; // 结果行记录数 (ROW_RECORD)
	private long allFileSize; // 所有文件的大小(单位:MB)(ALL_FILE_SIZE)
	private String execCMD; // 执行命令(EXEC_CMD)
	private long mapInputCount; // Map输入数据量(MAP_INPUT_COUNT)
	private long reduceInputCount; // Reduce输入数据量(REDUCE_INPUT_COUNT)
	private long mapOutputCount; // Map输出数据量 (MAP_OUTPUT_COUNT)
	private long reduceOutputCount; // Reduce输出数据量(REDUCE_OUTPUT_COUNT)
	private String logMSG; // 日志信息 (LOG_MSG)
	private String queueName; // 队列名称

	/**
	 * 默认构造方法
	 */
	public MRJobRunLog() {
	}

	/**
	 * 
	 * @param logId
	 *            日志ID
	 * @param jobId
	 *            JOB ID号
	 * @param monthNo
	 *            执行的月份
	 * @param dataNo
	 *            参数的日期
	 * @param startDate
	 *            开始时间
	 * @param endDate
	 *            结束时间
	 * @param runFlag
	 *            运行状态
	 * @param rowRecord
	 *            结果行记录数
	 * @param allFileSize
	 *            所有文件的大小
	 * @param execCMD
	 *            执行命令
	 * @param logMSG
	 *            日志信息
	 */
	public MRJobRunLog(long logId, long jobId, String monthNo, String dataNo, Date startDate, Date endDate, int runFlag, long rowRecord,
			long allFileSize, String execCMD, String logMSG) {
		this.logId = logId;
		this.jobId = jobId;
		this.monthNo = monthNo;
		this.dataNo = dataNo;
		this.startDate = startDate;
		this.endDate = endDate;
		this.runFlag = runFlag;
		this.rowRecord = rowRecord;
		this.allFileSize = allFileSize;
		this.execCMD = execCMD;
		this.logMSG = logMSG;
	}

	public long getLogId() {
		return logId;
	}

	public void setLogId(int logId) {
		this.logId = logId;
	}

	public long getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public String getMonthNo() {
		return monthNo;
	}

	public void setMonthNo(String monthNo) {
		this.monthNo = monthNo;
	}

	public String getDataNo() {
		return dataNo;
	}

	public void setDataNo(String dataNo) {
		this.dataNo = dataNo;
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

	public long getRowRecord() {
		return rowRecord;
	}

	public void setRowRecord(long rowRecord) {
		this.rowRecord = rowRecord;
	}

	public long getAllFileSize() {
		return allFileSize;
	}

	public void setAllFileSize(int allFileSize) {
		this.allFileSize = allFileSize;
	}

	public String getExecCMD() {
		return execCMD;
	}

	public void setExecCMD(String execCMD) {
		this.execCMD = execCMD;
	}

	public long getMapInputCount() {
		return mapInputCount;
	}

	public void setMapInputCount(long mapInputCount) {
		this.mapInputCount = mapInputCount;
	}

	public long getReduceInputCount() {
		return reduceInputCount;
	}

	public void setReduceInputCount(long reduceInputCount) {
		this.reduceInputCount = reduceInputCount;
	}

	public long getMapOutputCount() {
		return mapOutputCount;
	}

	public void setMapOutputCount(long mapOutputCount) {
		this.mapOutputCount = mapOutputCount;
	}

	public long getReduceOutputCount() {
		return reduceOutputCount;
	}

	public void setReduceOutputCount(long reduceOutputCount) {
		this.reduceOutputCount = reduceOutputCount;
	}

	public String getLogMSG() {
		return logMSG;
	}

	public void setLogMSG(String logMSG) {
		this.logMSG = logMSG;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = null == queueName?"":queueName;
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("jobrun log infomation>> logId=");
		strBuilder.append(this.logId);
		strBuilder.append(", ");
		strBuilder.append("jobId=");
		strBuilder.append(this.jobId);
		strBuilder.append(", ");
		strBuilder.append("monthNo=");
		strBuilder.append(this.monthNo);
		strBuilder.append(", ");
		strBuilder.append("dataNo=");
		strBuilder.append(this.dataNo);
		strBuilder.append(", ");
		strBuilder.append("startDate=");
		strBuilder.append(StringUtil.dateToString(this.startDate, StringUtil.DATE_FORMAT_TYPE1));
		strBuilder.append(", ");
		strBuilder.append("endDate=");
		strBuilder.append(StringUtil.dateToString(this.endDate, StringUtil.DATE_FORMAT_TYPE1));
		strBuilder.append(", ");
		strBuilder.append("runFlag=");
		strBuilder.append(this.runFlag);
		strBuilder.append(", ");
		strBuilder.append("rowRecord=");
		strBuilder.append(this.rowRecord);
		strBuilder.append(", ");
		strBuilder.append("allFileSize=");
		strBuilder.append(this.allFileSize);
		strBuilder.append(", ");
		strBuilder.append("execCMD=");
		strBuilder.append(this.execCMD);
		strBuilder.append(", ");
		strBuilder.append("mapInputCount=");
		strBuilder.append(this.mapInputCount);
		strBuilder.append(", ");
		strBuilder.append("reduceInputCount=");
		strBuilder.append(this.reduceInputCount);
		strBuilder.append(", ");
		strBuilder.append("mapOutputCount=");
		strBuilder.append(this.mapOutputCount);
		strBuilder.append(", ");
		strBuilder.append("reduceOutputCount=");
		strBuilder.append(this.reduceOutputCount);
		strBuilder.append(", ");
		strBuilder.append("logMSG=");
		strBuilder.append(this.logMSG);
		strBuilder.append(", ");
		strBuilder.append("queueName=");
		strBuilder.append(this.queueName);

		return strBuilder.toString();
	}
}
