package com.ery.hadoop.mrddx.log;

public class MRColFileLog {
	public static final int STATUS_INIT = 0;
	public static final int STATUS_SUCCESS = 1;
	public static final int STATUS_FAIL = 2;
	public static final int STATUS_RERUN = 3; // 重新执行
	
	private long colLogid;
	private int colId;
	private String startTime;
	private String endTime;
	private int fileNum;
	private long fileTotalsize;
	private int status;// 0:运行中， 1:成功(全部成功), 1:失败
	private String queueName;
	private String jobCmd;

	public long getColLogid() {
		return colLogid;
	}

	public void setColLogid(long id) {
		this.colLogid = id;
	}

	public int getColId() {
		return colId;
	}

	public void setColId(int colId) {
		this.colId = colId;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public int getFileNum() {
		return fileNum;
	}

	public void setFileNum(int fileNum) {
		this.fileNum = fileNum;
	}

	public long getFileTotalsize() {
		return fileTotalsize;
	}

	public void setFileTotalsize(long fileTotalsize) {
		this.fileTotalsize = fileTotalsize;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName == null?"":queueName;
	}

	public String getJobCmd() {
		return jobCmd;
	}

	public void setJobCmd(String jobCmd) {
		this.jobCmd = jobCmd == null?"":jobCmd;
	}
}
