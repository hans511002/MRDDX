package com.ery.hadoop.mrddx.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.util.HDFSUtils.CompressCodec;

/**
 * Hive(输入和输出)配置信息
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-29
 * @version v1.0
 */
public class HiveConfiguration extends MRConfiguration {
	// hive驱动
	public static final String hiveDriverClass = "org.apache.hadoop.hive.jdbc.HiveDriver";

	/**
	 * 输入来自与Hive
	 */
	// 连接hive数据库的driver名称(使用默认即可)
	public static final String INPUT_HIVE_CONFIG_DRIVER_NAME = "input.mr.maperd.hive.config.driver.name";
	// 连接hive数据库的url
	public static final String INPUT_HIVE_CONFIG_URL = "input.mr.maperd.hive.config.url";
	// 连接hive数据库的用户名
	public static final String INPUT_HIVE_CONFIG_USER = "input.mr.maperd.hive.config.user";
	// 连接hive数据库的密码
	public static final String INPUT_HIVE_CONFIG_PASSWORD = "input.mr.maperd.hive.config.password";
	// 查询数据的sql语句
	public static final String INPUT_HIVE_QUERY = "input.mr.maperd.hive.query";

	/**
	 * 输出
	 */
	// 连接hive数据库的driver名称(使用默认即可)
	public static final String OUTPUT_HIVE_CONFIG_DRIVER_NAME = "output.mr.maperd.hive.config.driver.name";
	// 连接hive数据库的url
	public static final String OUTPUT_HIVE_CONFIG_URL = "output.mr.maperd.hive.config.url";
	// 连接hive数据库的用户名
	public static final String OUTPUT_HIVE_CONFIG_USER = "output.mr.maperd.hive.config.user";
	// 连接hive数据库的密码
	public static final String OUTPUT_HIVE_CONFIG_PASSWORD = "output.mr.maperd.hive.config.password";
	// 写入数据到hive的表名
	public static final String OUTPUT_TABLE = "output.mr.maperd.hive.table";
	// 对表的DDL操作, 可以多条语句
	public static final String OUTPUT_HIVE_DDL_HQL = "output.mr.maperd.hive.ddl.hql";
	// 是否压缩，true or false
	public static final String OUTPUT_HIVE_COMPRESS = "output.mr.maperd.hive.compress";
	// 压缩标识符，与HiveConfiguration.CompressCodec中名称对应
	public static final String OUTPUT_HIVE_COMPRESS_CODEC = "output.mr.maperd.hive.compress.codec";
	// 字段分隔符
	public static final String OUTPUT_HIVE_FIELD_SPLITCHARS = "output.mr.maperd.hive.field.split.chars";
	// 行分隔符（默认为）
	public static final String OUTPUT_HIVE_ROWS_SPLITCHARS = "output.mr.maperd.hive.rows.split.chars";
	// move文件的目录
	public static final String OUTPUT_HIVE_TARGET_PATH = "output.mr.mapred.hive.target.Path";

	/**
	 * 分区参数
	 */
	// 分区字段名称
	public static final String OUTPUT_HIVE_PARTITION_FIELD = "output.mr.mapred.hive.partition.field";
	// 分区输出的路径
	public static final String OUTPUT_HIVE_ORDER_TEMP_PATH = "output.mr.mapred.hive.partition.temp.path";
	// 分区输出的文件名称
	public static final String OUTPUT_HIVE_ORDER_FILENAME = "output.mr.mapred.hive.partition.filename.prefix";
	// 分区输出的缓存大小
	public static final String OUTPUT_HIVE_ORDER_WRITEBUFFERSIZE = "output.mr.mapred.hive.partition.write.buffersize";
	// 分区输出的文件MAX COUNT(单位:条数)，默认：-1表示不限制
	public static final String OUTPUT_HIVE_ORDER_FILEMAXCOUNT = "output.mr.mapred.hive.partition.file.maxcount";

	/**
	 * 系统配置
	 */
	private Configuration conf;

	/**
	 * 构造方法
	 * 
	 * @param job 配置信息
	 */
	public HiveConfiguration(Configuration job) {
		super(job);
		this.conf = job;
	}

	public Configuration getConf() {
		return conf;
	}

	// 输入
	/**
	 * 设置hive输入的系统参数
	 * 
	 * @param driverName 驱动名称
	 * @param url 连接地址
	 * @param username 用户名
	 * @param password 用户密码
	 */
	public void configureInputHive(String driverName, String url, String username, String password) {
		this.conf.set(INPUT_HIVE_CONFIG_DRIVER_NAME, driverName);
		this.conf.set(INPUT_HIVE_CONFIG_URL, url);
		this.conf.set(INPUT_HIVE_CONFIG_USER, username);
		this.conf.set(INPUT_HIVE_CONFIG_PASSWORD, password);
	}

	/**
	 * 获取hive的driver name
	 */
	public String getInPutHiveConfigDriverName() {
		return this.conf.get(INPUT_HIVE_CONFIG_DRIVER_NAME, hiveDriverClass);
	}

	/**
	 * 获取hive的url
	 * 
	 * @return url 地址
	 */
	public String getInPutHiveConfigUrl() {
		return this.conf.get(INPUT_HIVE_CONFIG_URL);
	}

	/**
	 * 获取hive的用户名
	 * 
	 * @return 用户名
	 */
	public String getInPutHiveConfigUser() {
		return this.conf.get(INPUT_HIVE_CONFIG_USER);
	}

	/**
	 * 获取hive的密码
	 * 
	 * @return 密码
	 */
	public String getInPutHiveConfigPassword() {
		return this.conf.get(INPUT_HIVE_CONFIG_PASSWORD);
	}

	/**
	 * 设置查询sql
	 * 
	 * @param query
	 */
	public void setInputHiveQuery(String query) {
		this.conf.set(INPUT_HIVE_QUERY, query);
	}

	public String getInputHiveQuery() {
		return conf.get(INPUT_HIVE_QUERY, null);
	}

	/**
	 * Returns a connection object o the DB
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Connection getInputConnection() throws ClassNotFoundException, SQLException {
		String driverName = this.getInPutHiveConfigDriverName();
		String url = this.getInPutHiveConfigUrl();
		String user = this.getInPutHiveConfigUser();
		String password = this.getInPutHiveConfigPassword();

		Class.forName(driverName);
		if (null == user) {
			return DriverManager.getConnection(url);
		} else {
			return DriverManager.getConnection(url, user, password);
		}
	}

	// 输出
	/**
	 * 设置hive用户名和密码配置
	 * 
	 * @param conf 配置对象
	 * @param driverName 驱动
	 * @param url 连接地址
	 * @param username 用户名
	 * @param password 密码
	 */
	public void configureOutputHive(Configuration conf, String driverName, String url, String username, String password) {
		this.conf.set(OUTPUT_HIVE_CONFIG_DRIVER_NAME, driverName);
		this.conf.set(OUTPUT_HIVE_CONFIG_URL, url);
		this.conf.set(OUTPUT_HIVE_CONFIG_USER, username);
		this.conf.set(OUTPUT_HIVE_CONFIG_PASSWORD, password);
	}

	/**
	 * 设置表名
	 * 
	 * @param tableName
	 */
	public void setOutputHiveTableName(String tableName) {
		this.conf.set(OUTPUT_TABLE, tableName);
	}

	public String getOutputHiveTableName() {
		return this.conf.get(OUTPUT_TABLE);
	}

	/**
	 * 获取hive的driver name
	 */
	public String getOutPutHiveConfigDriverName() {
		return this.conf.get(OUTPUT_HIVE_CONFIG_DRIVER_NAME, hiveDriverClass);
	}

	/**
	 * 获取hive的url
	 * 
	 * @return url
	 */
	public String getOutPutHiveConfigUrl() {
		return this.conf.get(OUTPUT_HIVE_CONFIG_URL);
	}

	/**
	 * 获取hive的用户名
	 * 
	 * @return 用户名
	 */
	public String getOutPutHiveConfigUser() {
		return this.conf.get(OUTPUT_HIVE_CONFIG_USER);
	}

	/**
	 * 获取hive的密码
	 * 
	 * @return 密码
	 */
	public String getOutPutHiveConfigPassword() {
		return this.conf.get(OUTPUT_HIVE_CONFIG_PASSWORD);
	}

	/**
	 * ddl执行语句
	 * 
	 * @param ddlHQL
	 */
	public void setOutputHiveExecuteDDLHQL(String ddlHQL) {
		this.conf.set(OUTPUT_HIVE_DDL_HQL, ddlHQL);
	}

	public String getOutputHiveExecuteDDLHQL() {
		return this.conf.get(OUTPUT_HIVE_DDL_HQL);
	}

	/**
	 * 设置压缩类型
	 * 
	 * @param compressType
	 */
	public void setOutputHiveCompress(boolean compress) {
		this.conf.setBoolean(OUTPUT_HIVE_COMPRESS, compress);
	}

	public boolean getOutputHiveCompress() {
		return this.conf.getBoolean(OUTPUT_HIVE_COMPRESS, false);
	}

	/**
	 * 设置压缩类型
	 * 
	 * @param compressCodec
	 */
	public void setOutputHiveCompressCodec(String compressCodec) {
		this.conf.set(OUTPUT_HIVE_COMPRESS_CODEC, compressCodec);
	}

	public String getOutputHiveCompressCodec() {
		return this.conf.get(OUTPUT_HIVE_COMPRESS_CODEC, CompressCodec.DefaultCodec.toString());
	}

	/**
	 * 设置字段分隔符
	 * 
	 * @param splitChars 字段分隔符
	 */
	public void setOutputHiveFileFieldSplitChars(String fieldSplitChars) {
		this.conf.set(OUTPUT_HIVE_FIELD_SPLITCHARS, fieldSplitChars);
	}

	public String getOutputHiveFileFieldSplitChars() {
		return this.conf.get(OUTPUT_HIVE_FIELD_SPLITCHARS, ",");
	}

	/**
	 * 设置行分隔符
	 * 
	 * @param splitChars 行分隔符
	 */
	public void setOutputHiveFileRowsSplitChars(String rowsSplitChars) {
		this.conf.set(OUTPUT_HIVE_ROWS_SPLITCHARS, rowsSplitChars);
	}

	public String getOutputHiveFileRowsSplitChars() {
		return this.conf.get(OUTPUT_HIVE_ROWS_SPLITCHARS, "\n");
	}

	public String getOutputTargetFilePath() {
		return conf.get(OUTPUT_HIVE_TARGET_PATH, "");
	}

	public void setOutputTargetFilePath(String path) {
		conf.set(OUTPUT_HIVE_TARGET_PATH, path);
	}

	public String[] getOutputHivePartitionField() {
		return conf.getStrings(OUTPUT_HIVE_PARTITION_FIELD, new String[0]);
	}

	public void setOutputHivePartitionField(String path) {
		conf.set(OUTPUT_HIVE_PARTITION_FIELD, path);
	}

	public String getOutputHiveOrderTempPath() {
		return conf.get(OUTPUT_HIVE_ORDER_TEMP_PATH, "");
	}

	public void setOutputHiveOrderTempPath(String path) {
		conf.set(OUTPUT_HIVE_ORDER_TEMP_PATH, path);
	}

	public String getOutputHiveOrderFileNamePrefix() {
		return conf.get(OUTPUT_HIVE_ORDER_FILENAME, "");
	}

	public void setOutputHiveOrderFileNamePrefix(String prefix) {
		conf.set(OUTPUT_HIVE_ORDER_FILENAME, prefix);
	}

	public int getOutputHiveOrderWriteBufferSize() {
		return conf.getInt(OUTPUT_HIVE_ORDER_WRITEBUFFERSIZE, 4096);
	}

	public void setOutputHiveOrderWriteBufferSize(int size) {
		conf.setInt(OUTPUT_HIVE_ORDER_WRITEBUFFERSIZE, size);
	}

	public long getOutputHiveOrderFileMaxCount() {
		return conf.getLong(OUTPUT_HIVE_ORDER_FILEMAXCOUNT, -1);
	}

	public void setOutputHiveOrderFileMaxCount(long maxSize) {
		conf.setLong(OUTPUT_HIVE_ORDER_FILEMAXCOUNT, maxSize);
	}

	/**
	 * Returns a connection object o the DB
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Connection getOutputConnection() throws ClassNotFoundException, SQLException {
		String driverName = this.getOutPutHiveConfigDriverName();
		String url = this.getOutPutHiveConfigUrl();
		String user = this.getOutPutHiveConfigUser();
		String password = this.getOutPutHiveConfigPassword();

		Class.forName(driverName);
		if (null == user) {
			return DriverManager.getConnection(url, "", "");
		} else {
			return DriverManager.getConnection(url, user, password);
		}
	}

}
