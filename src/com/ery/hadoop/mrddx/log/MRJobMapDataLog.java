package com.ery.hadoop.mrddx.log;


public class MRJobMapDataLog {
	public static final int STATUS_INIT = 0; // 初始化
	public static final int STATUS_DOING = 1; // 处理中 
	public static final int STATUS_SUCCESS = 2; // 成功
	public static final int STATUS_FAILD = 3; // 失败
	public static final int STATUS_DEL = 4; // 已删除(只记录通过配置删除)
	public static final int DATATYPE_TXT = 0; // txt文本
	public static final int DATATYPE_SEQ = 1; // seq文本
	public static final int DATATYPE_RCF = 2; // rcfile文本
	
	private long id;
	private String jobid;
	private long jobLogId;
	private String fileid;
	private int dataSourceId;
	private int dataType;
	private String filePath;
	private long fileSize;
	private String startTime;
	private String endTime;
	private long totalCount;
	private long successCount;
	private long failCount;
	private String filterPath;
	private int status;
	
	private String hdfsAddress;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

	public long getJobLogId() {
		return jobLogId;
	}

	public void setJobLogId(long jobLogId) {
		this.jobLogId = jobLogId;
	}

	public String getFileid() {
		return fileid;
	}

	public void setFileid(String fileid) {
		this.fileid = fileid;
	}

	public int getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(int dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
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

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public long getSuccessCount() {
		return successCount;
	}

	public void setSuccessCount(long successCount) {
		this.successCount = successCount;
	}

	public long getFailCount() {
		return failCount;
	}

	public void setFailCount(long failCount) {
		this.failCount = failCount;
	}

	public String getFilterPath() {
		return filterPath;
	}

	public void setFilterPath(String filterPath) {
		this.filterPath = filterPath;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getHdfsAddress() {
		return hdfsAddress;
	}

	public void setHdfsAddress(String hdfsAddress) {
		this.hdfsAddress = hdfsAddress;
	}
}
