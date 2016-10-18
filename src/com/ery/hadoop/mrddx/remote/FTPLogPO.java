package com.ery.hadoop.mrddx.remote;

public class FTPLogPO {
	public static final int STATUS_INIT = 0; // 开始状态
	public static final int STATUS_SUCCESS = 1; // 成功状态
	public static final int STATUS_FAIL = 2; // 失败状态
	public static final int STATUS_REPEAT = 3; // 重复开始状态

	private long colId;
	private long colLogId;
	private long colFileDetailId;
	private int status;
	private String inputFileLastModifyTime;
	private String outputFileMsg;
	private String inputFileMsg;
	private String runDateTime;
	private long recordInvalidCount;
	
	public long getColId() {
		return colId;
	}

	public void setColId(long colId) {
		this.colId = colId;
	}

	public long getColLogId() {
		return colLogId;
	}

	public void setColLogId(long colLogId) {
		this.colLogId = colLogId;
	}

	public long getColFileDetailId() {
		return colFileDetailId;
	}

	public void setColFileDetailId(long colFileDetailId) {
		this.colFileDetailId = colFileDetailId;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getInputFileLastModifyTime() {
		return inputFileLastModifyTime;
	}

	public void setInputFileLastModifyTime(String inputFileLastModifyTime) {
		this.inputFileLastModifyTime = inputFileLastModifyTime;
	}

	public String getOutputFileMsg() {
		return outputFileMsg;
	}

	public void setOutputFileMsg(String outputFileMsg) {
		this.outputFileMsg = outputFileMsg;
	}

	public String getInputFileMsg() {
		return inputFileMsg;
	}

	public void setInputFileMsg(String inputFileMsg) {
		this.inputFileMsg = inputFileMsg;
	}

	public String getRunDateTime() {
		return runDateTime;
	}

	public void setRunDateTime(String runDateTime) {
		this.runDateTime = runDateTime;
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
		str.append(", jobId=");
		str.append(colId);
		str.append(", status=");
		str.append(status);
		str.append(", inputFileCreateDate=");
		str.append(inputFileLastModifyTime);
		str.append(", outputFileMsg=");
		str.append(outputFileMsg);
		str.append(", inputFileMsg=");
		str.append(inputFileMsg);
		str.append(", filterCount=");
		str.append(recordInvalidCount);
		return str.toString();
	}
}
