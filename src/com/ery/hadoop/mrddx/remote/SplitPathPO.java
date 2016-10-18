package com.ery.hadoop.mrddx.remote;

public class SplitPathPO {
	private Object obj;
	private String record = "";
	private long fileLogId;

	public Object getObj() {
		return obj;
	}

	public void setObj(Object obj) {
		this.obj = obj;
	}

	public String getRecord() {
		return record;
	}

	public void setRecord(String record) {
		this.record = record;
	}

	public void setFileLogId(long id) {
		this.fileLogId = id;
	}
	
	public long getFileLogId() {
		return fileLogId;
	}
	
	@Override
	public String toString() {
		return record;
	}
}
