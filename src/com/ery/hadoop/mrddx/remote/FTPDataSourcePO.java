package com.ery.hadoop.mrddx.remote;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class FTPDataSourcePO implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9213231998994363344L;
	private int id;
	private String protocol = "";
	private String userName = "";
	private String password = "";
	private String ip = "";
	private String port = "";
	Map<String, String> othParams = null;

	public void putPar(String key, String val) {
		if (othParams == null) {
			othParams = new HashMap<String, String>();
		}
		othParams.put(key, val);
	}

	public String get(String key, String def) {
		if (othParams == null || !othParams.containsKey(key))
			return def;
		return othParams.get(key);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getSourceSystemOnlyFlag() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(this.getProtocol());
		strBuilder.append(":");
		strBuilder.append(null == this.getIp() ? "" : this.getIp());
		strBuilder.append(":");
		strBuilder.append(null == this.getPort() ? "" : this.getPort());
		strBuilder.append(":");
		strBuilder.append(null == this.getUserName() ? "" : this.getUserName());
		strBuilder.append(":");
		strBuilder.append(null == this.getPassword() ? "" : this.getPassword());
		return strBuilder.toString();
	}

	@Override
	public String toString() {
		return this.getSourceSystemOnlyFlag();
	}

	public String getMetaInfo() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("协议类型：" + this.getProtocol());
		strBuilder.append("，服务器地址：" + (null == this.getIp() ? "" : this.getIp()) + ":" +
				(null == this.getPort() ? "" : this.getPort()));
		strBuilder.append("，用户名：" + null == this.getUserName() ? "" : this.getUserName());
		return strBuilder.toString();
	}
}
