package com.ery.hadoop.mrddx.remote;

public interface Contant {
	// protocol type
	public static final String PROTOCOL_LOCAL = "LOCAL"; // 本地主机
	public static final String PROTOCOL_LOCALHDFS = "LOCALHDFS";// 本地HDFS
	public static final String PROTOCOL_REMOREHDFS = "REMOREHDFS";// 远程HDFS
	public static final String PROTOCOL_FTP = "FTP";// 远程FTP
	public static final String PROTOCOL_SFTP = "SFTP";// 远程SFTP

	public static final String SYS_ZK_ADDRESS = "sys.zk.job.address"; // zk的连接地址
	public static final String SYS_ZK_ROOT_PATH = "sys.zk.job.root.path"; // job下zk的根路径
	public static final String SYS_ZK_JOB_NAME = "sys.zk.job.job.name"; // job下zk的名称
	public static final String SYS_ZK_MONITOR_FILE_MIN_SIZE = "512"; // MB
}
