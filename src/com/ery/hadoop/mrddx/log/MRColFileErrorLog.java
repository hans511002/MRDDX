package com.ery.hadoop.mrddx.log;

public class MRColFileErrorLog {
	public static final int TYPE_DOWN = 0;
	public static final int TYPE_UP = 1;
	private long colLogId;
	private String creatTime;
	private String protocol;
	private String ip;
	private int port;
	private String username;
	private String rootpath;
	private int type;// 0:下载, 1:上传
	private String msg;

	public long getColLogId() {
		return colLogId;
	}

	public void setColLogId(long colLogId) {
		this.colLogId = colLogId;
	}

	public String getCreatTime() {
		return creatTime;
	}

	public void setCreatTime(String creatTime) {
		this.creatTime = creatTime;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getRootpath() {
		return rootpath;
	}

	public void setRootpath(String rootpath) {
		this.rootpath = rootpath;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}
}
