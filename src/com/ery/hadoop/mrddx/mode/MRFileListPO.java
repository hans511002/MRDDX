package com.ery.hadoop.mrddx.mode;

/**
 * 文件列表对象

 *
 */
public class MRFileListPO {
	private String fileId; // 文件id
	private int dataSourceId; // 文件所属数据源id
	private String filePath; // 文件路径
	private long fileSize; // 文件大小
	private String recordTime; // MR记录时间 yyyy-MM-dd HH:mm:ss
	private String recordMonth;// MR记录月份 yyyy-MM-dd

	public String getFileId() {
		return fileId;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}

	public int getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(int dataSourceId) {
		this.dataSourceId = dataSourceId;
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

	public String getRecordTime() {
		return recordTime;
	}

	public void setRecordTime(String recordTime) {
		this.recordTime = recordTime;
	}

	public String getRecordMonth() {
		return recordMonth;
	}

	public void setRecordMonth(String recordMonth) {
		this.recordMonth = recordMonth;
	}
}
