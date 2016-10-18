package com.ery.hadoop.mrddx.remote;

import java.text.ParseException;

import com.ery.hadoop.mrddx.util.StringUtil;

public class FileAttributePO {
	private String path;
	private long size;
	private String lastModifyTime;
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	public String getLastModifyTime() {
		return this.lastModifyTime;
	}
	public void setLastModifyTime(long lastModifyTime) {
		try {
			this.lastModifyTime = StringUtil.longToString(lastModifyTime, StringUtil.DATE_FORMAT_TYPE1);
		} catch (ParseException e) {
			e.printStackTrace();
			this.lastModifyTime = "-1";
		}
	}
	
	@Override
	public String toString() {
		return "path="+path+"  ,size="+size + " ,lastModifyTime="+lastModifyTime;
	}
}
