package com.ery.hadoop.mrddx;

/**
 * 数据类型
 * 
 * @author wanghao
 */
public interface DataTypeConstant {
	/**
	 * 文件(txt格式)
	 */
	public static final String FILETEXT = "FILETEXT";
	/**
	 * 文件(sequence格式)
	 */
	public static final String FILESEQUENCE = "FILESEQUENCE";
	/**
	 * 文件(RC格式)
	 */
	public static final String FILERCFILE = "FILERCFILE";
	/**
	 * Hive(txt格式)
	 */
	public static final String HIVETEXT = "HIVETEXT";
	/**
	 * Hive(sequence格式)
	 */
	public static final String HIVESEQUENCE = "HIVESEQUENCE";
	/**
	 * Hive(RC格式)
	 */
	public static final String HIVERCFILE = "HIVERCFILE";
	/**
	 * HBase
	 */
	public static final String HBASE = "HBASE";
	public static final String RDBNORM = "RDBNORM";
	public static final String PARTITION_SPLIT = "PARTITION_SPLIT";

	/**
	 * 数据库(MYSQL-行)
	 */
	public static final String MYSQLROW = "MYSQLROW";
	/**
	 * 数据库(MYSQL-数据类型)
	 */
	public static final String MYSQLDATA = "MYSQLDATA";
	/**
	 * 数据库(ORACLE-行)
	 */
	public static final String ORACLEROW = "ORACLEROW";
	/**
	 * 数据库(ORACLE-数据类型)
	 */
	public static final String ORACLEDATA = "ORACLEDATA";
	/**
	 * 数据库(ORACLE-分区)
	 */
	public static final String ORACLEPART = "ORACLEPART";
	/**
	 * FTP数据
	 */
	public static final String FTPDATA = "FTPDATA";
	public static final String VERTICA = "VERTICA";

	// JOB
	/**
	 * 文本(NHFILE)
	 */
	public static final String MRHFILE = "NHFILE";
	/**
	 * NHBase
	 */
	public static final String MRHBASE = "NHBASE";
	public static final String POSTGRESQL = "POSTGRESQL";
}
