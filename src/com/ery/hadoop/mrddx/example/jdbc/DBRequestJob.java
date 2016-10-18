package com.ery.hadoop.mrddx.example.jdbc;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.client.MRJOBClient;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.remote.DBSourceRecordPO;
import com.ery.hadoop.mrddx.remote.FTPConfiguration;
import com.ery.hadoop.mrddx.remote.FTPDataSourcePO;
import com.ery.hadoop.mrddx.remote.SourceUtil;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.base.support.jdbc.DataSourceImpl;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.DataSourceManager;

/**
 * 请求job客户的对象
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-17
 * @version v1.0
 */
public class DBRequestJob implements IRequestJob {
	// 存放JOB实际运行的参数值.
	private Map<String, String> mapJobParam;
	private int inputDataSourceId = -1;
	private int outputDataSourceId = -1;
	private int inputSourceTypeId = -1;
	private int outputSourceTypeId = -1;

	// 采集的任务参数(下载、上传)
	private static final String QUERY_FTP_JOB = "SELECT COL_ID, COL_NAME, COL_ORIGIN, COL_DATATYPE, COL_STATUS, COL_TASK_NUMBER, COL_TASK_PRIORITY, COL_SYS_INPUTPATH, COL_RUN_DATASOURCE, PLUGIN_CODE FROM MR_FTP_COL_JOB WHERE COL_ID=?";
	private static final String QUERY_FTP_JOB_PARAM = "SELECT COL_ID, INPUT_DATASOURCE_ID, INPUT_FILELST_TYPE, INPUT_FILELST_DATASOURCE_ID,INPUT_QUERY_SQL,INPUT_PATH,INPUT_FILE_RULE,INPUT_DOTYPE,INPUT_MOVE_PATH,INPUT_RENAME,INPUT_RENAME_RULE, NOTE, OUTPUT_DATASOURCE_ID, OUTPUT_PATH,OUTPUT_RENAME, OUTPUT_RENAME_RULE, OUTPUT_MOVE_PATH,IS_COMPRESS FROM MR_FTP_COL_JOBPARAM WHERE COL_ID = ?";
	// private static final String QUERY_FTP_FAIL_JOB =
	// "SELECT COL_ID, COL_NAME, COL_ORIGIN, COL_DATATYPE, COL_STATUS, COL_TASK_NUMBER, COL_TASK_PRIORITY, COL_SYS_INPUTPATH, COL_RUN_DATASOURCE FROM MR_FTP_COL_JOB WHERE  COL_ID =(SELECT DISTINCT MAX(A.COL_ID) FROM MR_FTP_COL_DETAIL_FILELOG A INNER JOIN COL_JOB B ON A.COL_ID = B.COL_ID WHERE  STATUS = 2 AND  B.COL_ORIGIN = ? AND (A.COL_LOG_ID,A.INPUT_PATH,A.INPUT_FILE_NAME) IN (SELECT MAX(COL_LOG_ID),INPUT_PATH,INPUT_FILE_NAME FROM MR_FTP_COL_DETAIL_FILELOG GROUP BY INPUT_PATH,INPUT_FILE_NAME))";
	// private static final String QUERY_FTP_FAIL_JOB_PARAM =
	// "SELECT DISTINCT M.COL_ID, M.INPUT_DATASOURCE_ID, M.INPUT_FILELST_TYPE, M.INPUT_FILELST_DATASOURCE_ID,M.INPUT_QUERY_SQL,M.INPUT_PATH,M.INPUT_FILE_RULE,M.INPUT_DOTYPE,M.INPUT_MOVE_PATH,M.INPUT_RENAME,M.INPUT_RENAME_RULE, M.NOTE, M.OUTPUT_DATASOURCE_ID, M.OUTPUT_PATH,M.OUTPUT_RENAME, M.OUTPUT_RENAME_RULE,M.IS_COMPRESS, M.OUTPUT_MOVE_PATH,N.COL_LOG_ID FROM MR_FTP_COL_JOBPARAM M INNER JOIN MR_FTP_COL_JOB A ON A.COL_ID = M.COL_ID AND A.COL_ORIGIN = ?  LEFT JOIN MR_FTP_COL_DETAIL_FILELOG N ON M.COL_ID=N.COL_ID WHERE M.COL_ID IN (SELECT DISTINCT A.COL_ID FROM MR_FTP_COL_DETAIL_FILELOG A WHERE  STATUS = 2 AND (A.COL_ID,A.INPUT_PATH,A.INPUT_FILE_NAME) IN (SELECT MAX(COL_ID),INPUT_PATH,INPUT_FILE_NAME FROM MR_FTP_COL_DETAIL_FILELOG GROUP BY INPUT_PATH,INPUT_FILE_NAME))";

	private static final String QUERY_FTP_FAIL_JOB = "SELECT COL_ID, COL_NAME, COL_ORIGIN, COL_DATATYPE, COL_STATUS, COL_TASK_NUMBER, COL_TASK_PRIORITY, COL_SYS_INPUTPATH, COL_RUN_DATASOURCE, PLUGIN_CODE FROM MR_FTP_COL_JOB WHERE COL_ID=?";
	private static final String QUERY_FTP_FAIL_JOB_PARAM = "SELECT DISTINCT M.COL_ID, M.INPUT_DATASOURCE_ID, M.INPUT_FILELST_TYPE, M.INPUT_FILELST_DATASOURCE_ID,M.INPUT_QUERY_SQL,M.INPUT_PATH,M.INPUT_FILE_RULE,M.INPUT_DOTYPE,M.INPUT_MOVE_PATH,M.INPUT_RENAME,M.INPUT_RENAME_RULE, M.NOTE, M.OUTPUT_DATASOURCE_ID, M.OUTPUT_PATH,M.OUTPUT_RENAME, M.OUTPUT_RENAME_RULE,M.IS_COMPRESS, M.OUTPUT_MOVE_PATH,N.COL_LOG_ID FROM MR_FTP_COL_JOBPARAM M INNER JOIN MR_FTP_COL_JOB A ON A.COL_ID = M.COL_ID AND A.COL_ORIGIN = ?  LEFT JOIN MR_FTP_COL_DETAIL_FILELOG N ON M.COL_ID=N.COL_ID WHERE M.COL_ID =?  AND (N.COL_LOG_ID, N.INPUT_PATH, N.INPUT_FILE_NAME) IN (SELECT MAX(COL_LOG_ID), INPUT_PATH, INPUT_FILE_NAME FROM MR_FTP_COL_DETAIL_FILELOG  GROUP BY INPUT_PATH, INPUT_FILE_NAME)";

	// 处理的任务参数
	private static final String QUERY_MR_JOB = "SELECT JOB_ID  , INPUT_DATA_SOURCE_ID, OUTPUT_DATA_SOURCE_ID, "
			+ "JOB_NAME  , JOB_PRIORITY  ,INPUT_DIR  ,MAP_TASKS ,REDUCE_TASKS  ,JOB_RUN_DATASOURCE,INPUT_PLUGIN_CODE, OUTPUT_PLUGIN_CODE FROM MR_JOB WHERE JOB_ID=?";
	private static final String QUERY_MR_DATA_SOURCE = "SELECT SOURCE_TYPE_ID from MR_DATA_SOURCE WHERE DATA_SOURCE_ID =?";
	private static final String QUERY_MR_DATA_SOURCE_PARAM = "SELECT PARAM_NAME, PARAM_VALUE FROM MR_DATA_SOURCE_PARAM WHERE DATA_SOURCE_ID = ?";
	private static final String QUERY_MR_SOURCE_TYPE_INPUT = "SELECT SOURCE_TYPE \"input.sys.mr.mapred.type\" FROM MR_SOURCE_TYPE WHERE SOURCE_CATE = 0 AND SOURCE_TYPE_ID = ?";
	private static final String QUERY_MR_SOURCE_TYPE_OUTPUT = "SELECT SOURCE_TYPE \"output.sys.mr.mapred.type\" FROM MR_SOURCE_TYPE WHERE SOURCE_CATE = 0 AND SOURCE_TYPE_ID = ?";
	private static final String QUERY_MR_SOURCE_PARAM_INPUT = "SELECT PARAM_NAME, DEFAULT_VALUE FROM MR_SOURCE_PARAM WHERE SOURCE_TYPE_ID = ? AND INPUT_OR_OUTPUT = 1";
	private static final String QUERY_MR_SOURCE_PARAM_OUTPUT = "SELECT PARAM_NAME, DEFAULT_VALUE FROM MR_SOURCE_PARAM WHERE SOURCE_TYPE_ID = ? AND INPUT_OR_OUTPUT = 2";
	private static final String QUERY_MR_JOB_PARAM = "SELECT PARAM_NAME, PARAM_VALUE FROM MR_JOB_PARAM WHERE JOB_ID = ?";

	/**
	 * 默认构造函数
	 */
	public DBRequestJob() {
		this.mapJobParam = new HashMap<String, String>();
	}

	public static String dsKey;

	/**
	 * 构造方法
	 * 
	 * @throws Exception
	 */
	public void init(int downFTPId, int jobId, int upFTPId, String url, String user, String password, boolean downfail,
			boolean upfail) throws Exception {
		LogUtils.info("sleep 10s");
		// Thread.sleep(10000);
		DataSourceImpl dsi = new DataSourceImpl(url, user, password);
		dsKey = user + "/" + password + "@" + url;
		DataSourceManager.addDataSource(dsKey, dsi);//
		int ftpFlag = -1;
		if (downFTPId > 0 || downfail) {
			ftpFlag = 0;// 下载
			this.setFTPJOB(dsKey, downFTPId, ftpFlag, downfail);
			this.setFTPJOBParam(dsKey, downFTPId, ftpFlag, downfail);// 对于下载进行初始化参数设置
		}

		if (jobId > 0) {
			this.setMRJOB(dsKey, jobId);
			this.inputSourceTypeId = this.getSourceId(dsKey, this.inputDataSourceId, QUERY_MR_DATA_SOURCE);
			this.outputSourceTypeId = this.getSourceId(dsKey, this.outputDataSourceId, QUERY_MR_DATA_SOURCE);
			this.mapJobParam.put(MRConfiguration.SYS_INPUT_DATA_SOURCE_ID, String.valueOf(this.inputSourceTypeId));
			this.mapJobParam.put(MRConfiguration.SYS_OUTPUT_DATA_SOURCE_ID, String.valueOf(this.outputSourceTypeId));
			this.setSourceTypeParam(dsKey, this.inputSourceTypeId, QUERY_MR_SOURCE_PARAM_INPUT);
			this.setSourceTypeParam(dsKey, this.outputSourceTypeId, QUERY_MR_SOURCE_PARAM_OUTPUT);
			this.setSourceType(dsKey, this.inputSourceTypeId, QUERY_MR_SOURCE_TYPE_INPUT);
			this.setSourceType(dsKey, this.outputSourceTypeId, QUERY_MR_SOURCE_TYPE_OUTPUT);
			this.setDataSource(dsKey, this.inputDataSourceId, "input.", QUERY_MR_DATA_SOURCE_PARAM);
			this.setDataSource(dsKey, this.outputDataSourceId, "output.", QUERY_MR_DATA_SOURCE_PARAM);
			this.setJobParam(dsKey, jobId, QUERY_MR_JOB_PARAM);
		}

		if (upFTPId > 0 || upfail) {
			ftpFlag = 1;// 上传
			this.setFTPJOB(dsKey, upFTPId, ftpFlag, upfail);
			this.setFTPJOBParam(dsKey, upFTPId, ftpFlag, upfail);
		}

		// 销毁连接池
		// ds.destroy();
		DataSourceManager.destroy();
	}

	@Override
	public Map<String, String> getJobParameter() {
		return mapJobParam;
	}

	public static Connection getConnection(String dsKey) {
		return DataSourceManager.getConnection(dsKey, Integer.MAX_VALUE);
	}

	/**
	 * 查询 下载任务参数
	 * 
	 * @param ds
	 * @param ftpId
	 * @param ftpFlag
	 * @throws SQLException
	 */
	public void setFTPJOB(String dsKey, int ftpId, int ftpFlag, boolean colfail) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		String prefix = null;
		switch (ftpFlag) {
		case 0:
			prefix = FTPConfiguration.DOWN_PREFIX;
			break;
		case 1:
			prefix = FTPConfiguration.UP_PREFIX;
			break;
		}
		try {
			statement = getConnection(dsKey).prepareStatement(QUERY_FTP_JOB);
			statement.setInt(1, ftpId);
			result = statement.executeQuery();
			ResultSetMetaData rsm = result.getMetaData();
			int columnCount = rsm.getColumnCount();
			if (result.next()) {
				for (int i = 1; i <= columnCount; i++) {
					String filedName = rsm.getColumnLabel(i).toUpperCase();
					if ("COL_ID".equals(filedName)) {
						this.mapJobParam.put(prefix + FTPConfiguration.COL_ID, result.getString(i));
					} else if ("COL_NAME".equals(filedName)) {
						if (colfail) {
							if (ftpFlag == 0) {
								this.mapJobParam.put(prefix + FTPConfiguration.COL_NAME, "下载全部失败");
							} else if (ftpFlag == 1) {
								this.mapJobParam.put(prefix + FTPConfiguration.COL_NAME, "上传全部失败");
							}
						} else {
							this.mapJobParam.put(prefix + FTPConfiguration.COL_NAME, result.getString(i));
						}
					} else if ("COL_ORIGIN".equals(filedName)) {
						this.mapJobParam.put(prefix + FTPConfiguration.COL_TYPE, result.getString(i));
					} else if ("COL_DATATYPE".equals(filedName)) {
						this.mapJobParam.put(prefix + FTPConfiguration.COL_DATATYPE, result.getString(i));
					} else if ("COL_STATUS".equals(filedName)) {
						this.mapJobParam.put(prefix + FTPConfiguration.COL_STATUS, result.getString(i));
					} else if ("COL_TASK_NUMBER".equals(filedName)) {
						this.mapJobParam.put(prefix + FTPConfiguration.COL_TASK_NUMBER, result.getString(i));
					} else if ("COL_TASK_PRIORITY".equals(filedName)) {
						this.mapJobParam.put(prefix + FTPConfiguration.COL_TASK_PRIORITY, result.getString(i));
					} else if ("COL_SYS_INPUTPATH".equals(filedName)) {
						this.mapJobParam.put(prefix + FTPConfiguration.COL_SYS_INPUTPATH, result.getString(i));
					} else if ("PLUGIN_CODE".equals(filedName)) {
						Object obj = result.getObject(i);
						if (obj instanceof oracle.sql.BLOB) {
							String s = StringUtil.convertBLOBtoString((oracle.sql.BLOB) obj);
							this.mapJobParam.put(MRConfiguration.SYS_INPUT_MAP_PLUGIN_CODE, s);
							this.mapJobParam.put(MRConfiguration.SYS_INPUT_MAP_PLUGIN_CLASSNAME,
									StringUtil.getClassName(s));
						}
					} else if ("COL_RUN_DATASOURCE".equals(filedName)) {
						Map<String, String> map = this.getDataSource(dsKey, result.getInt(i));
						for (String key : map.keySet()) {
							switch (ftpFlag) {
							case 0:
								if (MRConfiguration.MAPRED_FS_DEFAULT_NAME.equals(key)) {// 设置下载运行参数数据源
									this.mapJobParam.put(FTPConfiguration.MAPRED_DOWNFTP_FS_DEFAULT_NAME, map.get(key));
								} else if (MRConfiguration.MAPRED_JOB_TRACKER_NAME.equals(key)) {
									this.mapJobParam
											.put(FTPConfiguration.MAPRED_DOWNFTP_JOB_TRACKER_NAME, map.get(key));
									if (!this.mapJobParam.containsKey(FTPConfiguration.MAPRED_JOB_TRACKER_NAME))
										this.mapJobParam.put(FTPConfiguration.MAPRED_JOB_TRACKER_NAME, map.get(key));
								}
								break;
							case 1:
								if (MRConfiguration.MAPRED_FS_DEFAULT_NAME.equals(key)) {// 设置下载运行数据源
									this.mapJobParam.put(FTPConfiguration.MAPRED_UPFTP_FS_DEFAULT_NAME, map.get(key));
								} else if (MRConfiguration.MAPRED_JOB_TRACKER_NAME.equals(key)) {
									if (!this.mapJobParam.containsKey(FTPConfiguration.MAPRED_JOB_TRACKER_NAME))
										this.mapJobParam.put(FTPConfiguration.MAPRED_JOB_TRACKER_NAME, map.get(key));
									this.mapJobParam.put(FTPConfiguration.MAPRED_UPFTP_JOB_TRACKER_NAME, map.get(key));
								}
								break;
							default:
								this.mapJobParam.put(key, map.get(key));
							}
						}
					}
				}
			}
		} finally {
			close(result, statement);
		}
	}

	private void setFTPJOBParam(String dsKey, int ftpId, int ftpFlag, boolean downfail) throws Exception {
		ResultSet result = null;
		PreparedStatement statement = null;
		List<DBSourceRecordPO> listFTPPO = new ArrayList<DBSourceRecordPO>();
		try {
			statement = getConnection(dsKey).prepareStatement(QUERY_FTP_JOB_PARAM);
			statement.setInt(1, ftpId);
			result = statement.executeQuery();
			ResultSetMetaData rsm = result.getMetaData();
			int columnCount = rsm.getColumnCount();
			while (result.next()) {
				DBSourceRecordPO po = new DBSourceRecordPO();
				for (int i = 1; i <= columnCount; i++) {
					String filedName = rsm.getColumnLabel(i).toUpperCase();
					if ("COL_ID".equals(filedName)) {
						po.setColId(result.getInt(i));
					} else if ("INPUT_DATASOURCE_ID".equals(filedName)) {
						po.setInputDatasource(this.getDataSource(dsKey, po, result.getInt(i)));
					} else if ("INPUT_FILELST_TYPE".equals(filedName)) {
						po.setInputFilelstType(result.getInt(i));
					} else if ("INPUT_FILELST_DATASOURCE_ID".equals(filedName)) {
						po.setInputFilelstDatasourceParam(this.getDataSource(dsKey, result.getInt(i)));
					} else if ("INPUT_QUERY_SQL".equals(filedName)) {
						po.setInputQuerySql(result.getString(i));
					} else if ("INPUT_PATH".equals(filedName)) {
						po.setInputPath(result.getString(i));
					} else if ("INPUT_FILE_RULE".equals(filedName)) {
						po.setInputFileRule(result.getString(i));
					} else if ("INPUT_DOTYPE".equals(filedName)) {
						po.setInputDotype(result.getInt(i));
					} else if ("INPUT_MOVE_PATH".equals(filedName)) {
						po.setInputMovePath(result.getString(i));
					} else if ("INPUT_RENAME_RULE".equals(filedName)) {
						po.setInputRenameRule(result.getString(i));
					} else if ("INPUT_RENAME".equals(filedName)) {
						po.setInputRename(result.getString(i));
					} else if ("NOTE".equals(filedName)) {
						po.setNote(result.getString(i));
					} else if ("OUTPUT_DATASOURCE_ID".equals(filedName)) {
						po.setOutputDatasource(this.getDataSource(dsKey, po, result.getInt(i)));
					} else if ("OUTPUT_PATH".equals(filedName)) {
						po.setOutputPath(result.getString(i));
					} else if ("OUTPUT_RENAME_RULE".equals(filedName)) {
						po.setOutputRenameRule(result.getString(i));
					} else if ("OUTPUT_RENAME".equals(filedName)) {
						po.setOutputRename(result.getString(i));
					} else if ("OUTPUT_MOVE_PATH".equals(filedName)) {
						po.setOutputMovePath(result.getString(i));
					} else if ("IS_COMPRESS".equals(filedName)) {
						po.setCompress(result.getInt(i));
					}
				}
				listFTPPO.add(po);
			}

			switch (ftpFlag) {
			case 0:
				this.mapJobParam.put(FTPConfiguration.FTP_DOWN_MR_MAPRED_PARAM, SourceUtil.serializable(listFTPPO));
				break;
			case 1:
				this.mapJobParam.put(FTPConfiguration.FTP_UP_MR_MAPRED_PARAM, SourceUtil.serializable(listFTPPO));
				break;
			}
		} finally {
			close(result, statement);
		}
	}

	/**
	 * 设置ftp的数据源参数
	 * 
	 * @param ds
	 * @param jobId
	 * @throws Exception
	 */
	public FTPDataSourcePO getDataSource(String dsKey, DBSourceRecordPO po, int datasourceId) throws Exception {
		ResultSet result = null;
		PreparedStatement statement = null;
		FTPDataSourcePO fdsp = null;
		try {
			statement = getConnection(dsKey).prepareStatement(QUERY_MR_DATA_SOURCE_PARAM);
			statement.setInt(1, datasourceId);
			result = statement.executeQuery();
			fdsp = new FTPDataSourcePO();
			fdsp.setId(datasourceId);
			while (result.next()) {
				if ("mr.mapred.ftp.protocol".equalsIgnoreCase(result.getString(1))) {
					fdsp.setProtocol(result.getString(2));
				} else if ("mr.mapred.ftp.ip".equalsIgnoreCase(result.getString(1))) {
					fdsp.setIp(result.getString(2));
				} else if ("mr.mapred.ftp.port".equalsIgnoreCase(result.getString(1))) {
					fdsp.setPort(result.getString(2));
				} else if ("mr.mapred.ftp.username".equalsIgnoreCase(result.getString(1))) {
					fdsp.setUserName(result.getString(2));
				} else if ("mr.mapred.ftp.password".equalsIgnoreCase(result.getString(1))) {
					fdsp.setPassword(result.getString(2));
				} else {
					this.mapJobParam.put(result.getString(1), result.getString(2));
				}
			}
		} catch (Exception e) {
			throw new Exception(e);
		} finally {
			close(result, statement);
		}

		return fdsp;
	}

	/**
	 * 设置ftp的数据源参数
	 * 
	 * @param ds
	 * @param jobId
	 * @throws Exception
	 */
	public Map<String, String> getDataSource(String dsKey, int datasourceId) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		Map<String, String> map = new HashMap<String, String>();
		try {
			statement = getConnection(dsKey).prepareStatement(QUERY_MR_DATA_SOURCE_PARAM);
			statement.setInt(1, datasourceId);
			result = statement.executeQuery();
			while (result.next()) {
				map.put(result.getString(1), result.getString(2));
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		} finally {
			close(result, statement);
		}

		return map;
	}

	/**
	 * 查询MR_JOB表
	 * 
	 * @param ds
	 *            数据源
	 * @param jobId
	 *            jobId
	 * @throws SQLException
	 *             SQL异常
	 */
	public void setMRJOB(String dsKey, int jobId) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		try {
			statement = getConnection(dsKey).prepareStatement(QUERY_MR_JOB);
			statement.setInt(1, jobId);
			result = statement.executeQuery();
			ResultSetMetaData rsm = result.getMetaData();
			int columnCount = rsm.getColumnCount();
			while (result.next()) {
				for (int i = 1; i <= columnCount; i++) {
					String filedName = rsm.getColumnLabel(i).toUpperCase();
					if ("JOB_RUN_DATASOURCE".equals(filedName)) {// 设置运行job数据源
						this.mapJobParam.putAll(this.getDataSource(dsKey, result.getInt(i)));
					} else if ("INPUT_PLUGIN_CODE".equals(filedName)) {
						Object obj = result.getObject(i);
						if (obj instanceof oracle.sql.BLOB) {
							String s = StringUtil.convertBLOBtoString((oracle.sql.BLOB) obj);
							this.mapJobParam.put(MRConfiguration.SYS_INPUT_MAP_PLUGIN_CODE, s);
							this.mapJobParam.put(MRConfiguration.SYS_INPUT_MAP_PLUGIN_CLASSNAME,
									StringUtil.getClassName(s));
						}
					} else if ("OUTPUT_PLUGIN_CODE".equals(filedName)) {
						Object obj = result.getObject(i);
						if (obj instanceof oracle.sql.BLOB) {
							String s = StringUtil.convertBLOBtoString((oracle.sql.BLOB) obj);
							this.mapJobParam.put(MRConfiguration.SYS_OUTPUT_REDUCE_PLUGIN_CODE, s);
							this.mapJobParam.put(MRConfiguration.SYS_OUTPUT_REDUCE_PLUGIN_CLASSNAME,
									StringUtil.getClassName(s));
						}
					} else if ("JOB_NAME".equals(filedName)) {
						this.mapJobParam.put(MRConfiguration.MAPRED_JOB_NAME, result.getString(i));
					} else if ("JOB_PRIORITY".equals(filedName)) {
						this.mapJobParam.put(MRConfiguration.MAPRED_JOB_PRIORITY, result.getString(i));
					} else if ("INPUT_DIR".equals(filedName)) {
						this.mapJobParam.put(MRConfiguration.MAPRED_INPUT_DIR, result.getString(i));
					} else if ("MAP_TASKS".equals(filedName)) {
						this.mapJobParam.put(MRConfiguration.MAPRED_MAP_TASKS, result.getString(i));
					} else if ("REDUCE_TASKS".equals(filedName)) {
						this.mapJobParam.put(MRConfiguration.MAPRED_REDUCE_TASKS, result.getString(i));
					} else if ("JOB_ID".equals(filedName)) {
						this.mapJobParam.put(MRConfiguration.SYS_JOB_ID, result.getString(i));
					} else {
						this.mapJobParam.put(filedName, result.getString(i));
					}
				}
			}

			if (this.mapJobParam.containsKey("INPUT_DATA_SOURCE_ID")) {
				this.inputDataSourceId = Integer.parseInt(this.mapJobParam.remove("INPUT_DATA_SOURCE_ID"));
			}
			if (this.mapJobParam.containsKey("OUTPUT_DATA_SOURCE_ID")) {
				this.outputDataSourceId = Integer.parseInt(this.mapJobParam.remove("OUTPUT_DATA_SOURCE_ID"));
			}
		} finally {
			close(result, statement);
		}
	}

	/**
	 * 查询输入的系统参数
	 * 
	 * @param ds
	 *            数据源
	 * @param sourceTypeId
	 *            资源类型id
	 * @param query
	 *            查询语句
	 * @throws SQLException
	 *             SQL异常
	 */
	public int getSourceId(String dsKey, int dataSourceId, String query) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		try {
			statement = getConnection(dsKey).prepareStatement(query);
			statement.setInt(1, dataSourceId);
			result = statement.executeQuery();
			result.next();
			return result.getInt(1);
		} finally {
			close(result, statement);
		}
	}

	/**
	 * 查询输入的系统参数
	 * 
	 * @param ds
	 *            数据源
	 * @param sourceTypeId
	 *            资源类型id
	 * @param query
	 *            查询语句
	 * @throws SQLException
	 *             SQL异常
	 */
	public void setDataSource(String dsKey, int dataSourceId, String perfix, String query) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		try {
			statement = getConnection(dsKey).prepareStatement(query);
			statement.setInt(1, dataSourceId);
			result = statement.executeQuery();
			while (result.next()) {
				this.mapJobParam.put(perfix + result.getString(1), result.getString(2));
			}
		} finally {
			close(result, statement);
		}
	}

	/**
	 * 查询输入的系统参数
	 * 
	 * @param ds
	 *            数据源
	 * @param sourceTypeId
	 *            资源类型id
	 * @param query
	 *            查询语句
	 * @throws SQLException
	 *             SQL异常
	 */
	public void setSourceType(String dsKey, int sourceTypeId, String query) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		try {
			statement = getConnection(dsKey).prepareStatement(query);
			statement.setInt(1, sourceTypeId);
			result = statement.executeQuery();
			ResultSetMetaData rsm = result.getMetaData();
			int columnCount = rsm.getColumnCount();
			while (result.next()) {
				for (int i = 1; i <= columnCount; i++) {
					this.mapJobParam.put(rsm.getColumnLabel(i), result.getString(i));
				}
			}
		} finally {
			close(result, statement);
		}
	}

	/**
	 * 查询输入的系统参数
	 * 
	 * @param ds
	 *            数据源
	 * @param sourceTypeId
	 *            资源类型id
	 * @param query
	 *            查询语句
	 * @throws SQLException
	 *             SQL异常
	 */
	public void setSourceTypeParam(String dsKey, int sourceTypeId, String query) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		try {
			statement = getConnection(dsKey).prepareStatement(query);
			statement.setInt(1, sourceTypeId);
			result = statement.executeQuery();
			while (result.next()) {
				this.mapJobParam.put(result.getString(1), result.getString(2));
			}
		} finally {
			close(result, statement);
		}
	}

	/**
	 * 查询输入的系统参数
	 * 
	 * @param ds
	 *            数据源
	 * @param jobId
	 *            job id
	 * @param query
	 *            查询语句
	 * @throws SQLException
	 *             SQL异常
	 */
	public void setJobParam(String dsKey, int jobId, String query) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		try {
			statement = getConnection(dsKey).prepareStatement(query);
			statement.setInt(1, jobId);
			result = statement.executeQuery();
			while (result.next()) {
				this.mapJobParam.put(result.getString(1), result.getString(2));
			}
		} finally {
			close(result, statement);
		}
	}

	/**
	 * 关闭资源
	 * 
	 * @param result
	 *            结果集
	 * @param statement
	 *            statement对象
	 */
	private void close(ResultSet result, PreparedStatement statement) {
		if (result != null) {
			try {
				result.close();
			} catch (Throwable t) {
				LogUtils.debug("关闭ResultSet失败", t);
			}
		}

		if (statement != null) {
			try {
				statement.close();
			} catch (Throwable t) {
				LogUtils.debug("关闭Statement失败", t);
			}
		}
	}

	/**
	 * 入口函数
	 * 
	 * @param args
	 *            参数
	 * @throws Exception
	 *             异常
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: -jobid <jobid>");
			System.out.println("where COMMAND is one of:");
			System.out.println("-downid       下载采集JOB ID号:OTHER TO HDFS");
			System.out.println("       可选参数：-downlogid    下载采集日志ID");
			System.out.println("       可选参数：-downforce    强制重复下载采集");
			System.out.println("       可选参数：-downfail     强制重复下载采集");
			System.out.println("-jobid        JOB ID号, 处理数据id号");
			System.out.println("       可选参数：-jobforce     强制执行JOB，相当于初始化执行，源数据为文件有效");
			System.out.println("       可选参数：-joblogid     重新处理某个JOB日志ID对应的失败数据，源数据为文件有效");
			System.out.println("       可选参数：-joblogtime   重新处理某个JOB日志ID对应时间段数据，源数据为文件有效");
			System.out.println("                  eg: -joblogtime 20130913095944 20130912095944");
			System.out.println("       可选参数：-jobfail      重新处理某个JOB日志ID对应所有失败的数据，源数据为文件有效");
			System.out.println("-upid         上传采集JOB ID号:HDFS TO OTHER");
			System.out.println("       可选参数：-uplogid      上传采集的日志ID");
			System.out.println("       可选参数：-upforce      强制重复上传采集");
			System.out.println("       可选参数：-upfail       强制重复下载采集");
			System.out.println("-debug        日志，1：打印job配置参数, 2:打印job解析后的参数(包含1)，3：打印map和redue的输入输出记录， 例如 -debug 1");
			System.out.println("-rownum       每个map输入条数限制(设置-debug参数时，才生效, 默认值：10)");
			System.out.println("-queue        所属队列名称，可选");
			System.out.println("-test         表示验证任务参数，并不实际运行任务");
			System.out.println("-d            时间参数, 例如，2013、201302、20130201、201302011416、20130201141658");
			System.out.println("                 参数的位数范围[4-14], 位数需被2整除; 时间处理后的可替换的宏变量：");
			System.out.println("                  YYYY   --年(四位，例如：2013)");
			System.out.println("                  YY     --年（后两位，例如：13）");
			System.out.println("                  MM     --月(两位，例如:05)");
			System.out.println("                  M      --月(当第一位为0时，只取第二位，否则取两位，例如:05，M=5)");
			System.out.println("                  DD     --日(两位，例如:18) ");
			System.out.println("                  D      --日(当第一位为0时，只取第二位，否则取两位，例如:18，D=18)");
			System.out.println("                  HH     --小时(两位，例如:23)");
			System.out.println("                  MI     --分(两位，例如:58)");
			System.out.println("                  SS     --秒(两位，例如:59)");
			System.out.println("");
			System.out.println("系统默认提供的宏变量：  ");
			System.out.println("N_YYYY        当前时间-年(四位)");
			System.out.println("N_YY          当前时间-年(后两位)");
			System.out.println("N_MM          当前时间-月(两位)");
			System.out.println("N_M           当前时间-月(当第一位为0时，只取第二位，否则取两位)");
			System.out.println("N_DD          当前时间-天(两位)");
			System.out.println("N_D           当前时间-天(当第一位为0时，只取第二位，否则取两位)");
			System.out.println("N_HH          当前时间-小时(两位)");
			System.out.println("N_MI          当前时间-分钟(两位)");
			System.out.println("N_SS          当前时间-秒(两位)");
			System.out.println("N_YYYYMMDD    当前时间-年月日(八位)");
			System.out.println("N_YYYYMM      当前时间-年月(六位)");
			System.out.println("N_YYYYMMP     当前时间的前一个月-年月(六位)");
			System.out.println("N_YYYYMMDDN   当前时间的后一天-年月日(八位)");
			System.out.println("N_YYYYMMN     当前时间的下一个月-年月(六位)");
			System.out.println("N_YYYYMMDDP   当前时间的前一天-年月日(八位)");
			System.out.println("-var1         固定宏变量1");
			System.out.println("-var2         固定宏变量2");
			System.out.println("-var3         固定宏变量3");
			System.out.println("-var4         固定宏变量4");
			System.out.println("-var5         固定宏变量5");
			System.out.println("-var6         固定宏变量6");
			System.out.println("-var7         固定宏变量7");
			System.out.println("-var8         固定宏变量8");
			System.out.println("-var9         固定宏变量9");
			return;
		}

		String cmd = StringUtil.toString(args, " ");
		int downFTPId = -1;
		long downFTPLogId = -1;
		boolean downforce = false;
		boolean downFail = false;

		int jobId = -1;
		boolean jobforce = false;
		int joblogid = -1;
		String joblogtime = null;
		boolean jobfail = false;
		int tempNum = 0;
		int joblogType = -1;

		int upFTPId = -1;
		long upFTPLogId = -1;
		boolean upforce = false;
		boolean upFail = false;

		int debug = -1;
		int rownum = -1;
		String queue = null;
		boolean test = false;
		Map<String, String> macroParam = new HashMap<String, String>();
		Map<String, String> pmacroParam = new HashMap<String, String>(); // 存放可以运用于任务的名称和值
		for (int i = 0; i < args.length; i++) {
			if (!("-test".equals(args[i]) || "-downforce".equals(args[i]) || "-upforce".equals(args[i]) ||
					"-jobforce".equals(args[i]) || "-jobfail".equals(args[i]) || "-downfail".equals(args[i]) || "-upfail"
						.equals(args[i]))) {
				if ((i + 1) >= args.length || null == args[i + 1] || args[i + 1].trim().length() <= 0) {
					throw new Exception("变量" + args[i] + ", 必需传入参数[" + args[i] + " <value>]");
				}
			}

			if ("-jobid".equalsIgnoreCase(args[i])) {
				jobId = Integer.parseInt(args[i + 1]);
				if (jobId <= 0) {
					throw new Exception("变量-jobid传入参数不正确，必须是大于0");
				}
				macroParam.put("jobid", String.valueOf(jobId));
				i++;
			}
			if ("-jobforce".equalsIgnoreCase(args[i])) {
				jobforce = true;
				tempNum++;
				joblogType = 1;
			} else if ("-joblogid".equalsIgnoreCase(args[i])) {
				joblogid = Integer.parseInt(args[i + 1]);
				if (joblogid <= 0) {
					throw new Exception("变量-joblogid传入参数不正确");
				}
				i++;
				tempNum++;
				joblogType = 2;
			} else if ("-joblogtime".equalsIgnoreCase(args[i])) {
				String tstart = StringUtil.stringToStrDate(args[i + 1]);
				String tend = StringUtil.stringToStrDate(args[i + 2]);
				if (tstart == null) {
					throw new Exception("变量-joblogtime传入参数开始时间不正确");
				}
				if (tend == null) {
					throw new Exception("变量-debug传入参数结束时间不正确");
				}
				joblogtime = tstart + "," + tend;
				i += 2;
				tempNum++;
				joblogType = 3;
			} else if ("-jobfail".equalsIgnoreCase(args[i])) {
				jobfail = true;
				tempNum++;
				joblogType = 4;
			} else if ("-debug".equalsIgnoreCase(args[i])) {
				debug = Integer.parseInt(args[i + 1]);
				if (!(debug >= 1 && debug <= 3)) {
					throw new Exception("变量-debug传入参数不正确，必须是[1-3]");
				}
				macroParam.put("debug", String.valueOf(debug));
				i++;
			} else if ("-rownum".equalsIgnoreCase(args[i])) {
				rownum = Integer.parseInt(args[i + 1]);
				if (!(rownum > 0)) {
					throw new Exception("变量-rownum传入参数不正确，必须大于0");
				}
				macroParam.put("rownum", String.valueOf(rownum));
				i++;
			} else if ("-queue".equalsIgnoreCase(args[i])) {
				queue = args[i + 1];
				if (null == queue || queue.trim().length() <= 0) {
					queue = null;
				}
				macroParam.put("queue", queue);
				i++;
			} else if ("-test".equalsIgnoreCase(args[i])) {
				test = true;
			} else if ("-downid".equalsIgnoreCase(args[i])) {
				downFTPId = Integer.parseInt(args[i + 1]);
				if (downFTPId <= 0) {
					throw new Exception("变量-downid传入参数不正确，必须大于0");
				}
				i++;
			} else if ("-downlogid".equalsIgnoreCase(args[i])) {
				downFTPLogId = Long.parseLong(args[i + 1]);
				if (downFTPLogId <= 0) {
					throw new Exception("变量-downlogid传入参数不正确，必须大于0");
				}
				i++;
			} else if ("-downforce".equalsIgnoreCase(args[i])) {
				downforce = true;
			} else if ("-downfail".equalsIgnoreCase(args[i])) {
				downFail = true;
			} else if ("-upid".equalsIgnoreCase(args[i])) {
				upFTPId = Integer.parseInt(args[i + 1]);
				if (upFTPId <= 0) {
					throw new Exception("变量-upid传入参数不正确，必须大于0");
				}
				i++;
			} else if ("-uplogid".equalsIgnoreCase(args[i])) {
				upFTPLogId = Long.parseLong(args[i + 1]);
				if (upFTPLogId <= 0) {
					throw new Exception("变量-upid传入参数不正确，必须大于0");
				}

				i++;
			} else if ("-upforce".equalsIgnoreCase(args[i])) {
				upforce = true;
			} else if ("-upfail".equalsIgnoreCase(args[i])) {
				upFail = true;
			} else if ("-d".equalsIgnoreCase(args[i])) {
				if (!Pattern.compile("[0-9]*").matcher(args[i + 1]).matches() || args[i + 1].length() > 14 ||
						args[i + 1].length() < 4) {
					throw new Exception("变量-d传入参数不正确，例如，2013、201302、20130201、201302011416、20130201141658");
				}

				if (args[i + 1].trim().length() % 2 != 0) {
					throw new Exception("变量-d传入参数不正确，必须是偶数");
				}

				String time = args[i + 1];
				boolean isOK = time.length() >= 4 ? true : false;
				if (isOK) {
					Integer yyyy = Integer.parseInt(time.substring(0, 4));
					if (yyyy < 1970) {
						throw new Exception("变量-d传入参数年份不正确");
					}
					macroParam.put("YY", time.substring(2, 4));
					macroParam.put("YYYY", time.substring(0, 4));
				} else {
					macroParam.put("YY", null);
					macroParam.put("YYYY", null);
				}

				isOK = time.length() >= 6 ? true : false;
				if (isOK) {
					Integer mm = Integer.parseInt(time.substring(4, 6));
					if (mm < 1 || mm > 12) {
						throw new Exception("变量-d传入参数月份不正确 -D20111102111211");
					}
					macroParam.put("MM", time.substring(4, 6));
					macroParam.put("M", String.valueOf(Integer.parseInt(time.substring(4, 6))));
				} else {
					macroParam.put("MM", null);
					macroParam.put("M", null);
				}

				isOK = time.length() >= 8 ? true : false;
				if (isOK) {
					Integer dd = Integer.parseInt(time.substring(6, 8));
					if (dd < 1 || dd > 31) {
						throw new Exception("变量-d传入参数日不正确");
					}
					macroParam.put("DD", time.substring(6, 8));
					macroParam.put("D", String.valueOf(Integer.parseInt(time.substring(6, 8))));
				} else {
					macroParam.put("DD", null);
					macroParam.put("D", null);
				}

				isOK = time.length() >= 10 ? true : false;
				if (isOK) {
					Integer hh = Integer.parseInt(time.substring(8, 10));
					if (hh < 0 || hh > 23) {
						throw new Exception("变量-d传入参数小时不正确");
					}
					macroParam.put("HH", time.substring(8, 10));
				} else {
					macroParam.put("HH", null);
				}

				isOK = time.length() >= 12 ? true : false;
				if (isOK) {
					Integer mi = Integer.parseInt(time.substring(10, 12));
					if (mi < 0 || mi > 59) {
						throw new Exception("变量-d传入参数分钟不正确");
					}
					macroParam.put("MI", time.substring(10, 12));
				} else {
					macroParam.put("MI", null);
				}

				isOK = time.length() >= 14 ? true : false;
				if (isOK) {
					Integer ss = Integer.parseInt(time.substring(12, 14));
					if (ss < 0 || ss > 59) {
						throw new Exception("变量-d传入参数秒不正确");
					}
					macroParam.put("SS", time.substring(12, 14));
				} else {
					macroParam.put("SS", null);
				}

				i++;
			} else if ("-var1".equalsIgnoreCase(args[i])) {
				macroParam.put("var1", args[i + 1]);
				i++;
			} else if ("-var2".equalsIgnoreCase(args[i])) {
				macroParam.put("var2", args[i + 1]);
				i++;
			} else if ("-var3".equalsIgnoreCase(args[i])) {
				macroParam.put("var3", args[i + 1]);
				i++;
			} else if ("-var4".equalsIgnoreCase(args[i])) {
				macroParam.put("var4", args[i + 1]);
				i++;
			} else if ("-var5".equalsIgnoreCase(args[i])) {
				macroParam.put("var5", args[i + 1]);
				i++;
			} else if ("-var6".equalsIgnoreCase(args[i])) {
				macroParam.put("var6", args[i + 1]);
				i++;
			} else if ("-var7".equalsIgnoreCase(args[i])) {
				macroParam.put("var7", args[i + 1]);
				i++;
			} else if ("-var8".equalsIgnoreCase(args[i])) {
				macroParam.put("var8", args[i + 1]);
				i++;
			} else if ("-var9".equalsIgnoreCase(args[i])) {
				macroParam.put("var9", args[i + 1]);
				i++;
			} else if ("-p".equalsIgnoreCase(args[i]) || "-d".equalsIgnoreCase(args[i])) {
				String tmpParam[] = StringUtil.getNameValue(args[i + 1], "=");
				if (tmpParam == null) {
					throw new Exception("变量-p传入参数不正确， 例如： -p def_name1=def_value1");
				}
				macroParam.put(tmpParam[0], tmpParam[1]);
				pmacroParam.put(tmpParam[0], tmpParam[1]);
				i++;
			}
		}

		// 设置调试日志的默认值
		if (null != macroParam.get("debug") && null == macroParam.get("rownum")) {
			macroParam.put("rownum", String.valueOf(10));
		} else if (null == macroParam.get("debug") && null != macroParam.get("rownum")) {
			macroParam.remove("rownum");
		}

		// 当前时间
		Date date = new Date();
		String n_date = StringUtil.dateToString(date, StringUtil.DATE_FORMAT_TYPE2);
		Calendar cDate = Calendar.getInstance();
		cDate.setTime(date);
		cDate.add(Calendar.DATE, -1); // 得到前一天
		String p_date = StringUtil.dateToString(cDate.getTime(), StringUtil.DATE_FORMAT_TYPE2);

		Calendar cNDate = Calendar.getInstance();
		cNDate.setTime(date);
		int day = cNDate.get(Calendar.DATE);
		cNDate.set(Calendar.DATE, day + 1); // 得到后一天
		String next_date = StringUtil.dateToString(cNDate.getTime(), StringUtil.DATE_FORMAT_TYPE2);

		Calendar cMonth = Calendar.getInstance();
		cMonth.setTime(date);
		cMonth.add(Calendar.MONTH, 0); // 得到前一个月
		String p_month = StringUtil.dateToString(cMonth.getTime(), StringUtil.DATE_FORMAT_TYPE2);

		Calendar cNMonth = Calendar.getInstance();
		cNMonth.setTime(date);
		int month = cNDate.get(Calendar.MONTH);
		cNMonth.set(Calendar.MONTH, month + 2); // 得到后一个月
		String next_month = StringUtil.dateToString(cNMonth.getTime(), StringUtil.DATE_FORMAT_TYPE2);

		macroParam.put("N_YY", n_date.substring(2, 4));
		macroParam.put("N_YYYY", n_date.substring(0, 4));
		macroParam.put("N_MM", n_date.substring(4, 6));
		// N_M如果是两位, 而参数中存在N_M, 则报错
		macroParam.put("N_M", String.valueOf(Integer.parseInt(n_date.substring(4, 6))));
		macroParam.put("N_DD", n_date.substring(6, 8));
		// N_D如果是两位, 而参数中存在N_D, 则报错
		macroParam.put("N_D", String.valueOf(Integer.parseInt(n_date.substring(6, 8))));
		macroParam.put("N_HH", n_date.substring(8, 10));
		macroParam.put("N_MI", n_date.substring(10, 12));
		macroParam.put("N_SS", n_date.substring(12, 14));
		macroParam.put("N_YYYYMM", n_date.substring(0, 6));
		macroParam.put("N_YYYYMMN", next_month.substring(0, 6));
		macroParam.put("N_YYYYMMP", p_month.substring(0, 6));
		macroParam.put("N_YYYYMMDD", n_date.substring(0, 8));
		macroParam.put("N_YYYYMMDDN", next_date.substring(0, 8));
		macroParam.put("N_YYYYMMDDP", p_date.substring(0, 8));

		DBRequestJob db = new DBRequestJob();
		Map<String, String> jobParam = db.getJobParameter();
		jobParam.put(MRConfiguration.INTERNAL_SYS_RUN_JOB_CMD, cmd);

		// 获取properties配置
		Properties prop = new Properties();
		InputStream in = db.getClass().getResourceAsStream("/mrddx.properties");
		System.out.println(db.getClass().getResource("/").toString());
		prop.load(in);

		Set<?> keyValue = prop.keySet();
		for (Iterator<?> it = keyValue.iterator(); it.hasNext();) {
			String key = (String) it.next();
			String value = (String) prop.get(key);
			jobParam.put(key, value);
		}

		String url = jobParam.get("sys.conf.jdbc.url");
		String user = jobParam.get("sys.conf.jdbc.username");
		String pwd = jobParam.get("sys.conf.jdbc.password");

		// 获取数据库配置
		db.init(downFTPId, jobId, upFTPId, url, user, pwd, downFail, upFail);

		// 处理特殊参数(输入参数、输出参数、输入输出关系参数的存在换行符的情况)
		special(jobParam);

		// 替换参数中的宏变量
		Iterator<String> iterator = jobParam.keySet().iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			String value = jobParam.get(key);
			if (null == key || null == value) {
				continue;
			}

			String tmp = value;
			Iterator<String> miterator = macroParam.keySet().iterator();
			while (miterator.hasNext()) {
				String mkey = miterator.next();
				String mvalue = macroParam.get(mkey);
				Pattern p = Pattern.compile("\\{(?i)" + mkey + "\\}");
				Matcher m = p.matcher(value);
				if (m.find()) {
					if (mvalue != null) {
						tmp = tmp.replaceAll("\\{(?i)" + mkey + "\\}", mvalue);
					} else {
						throw new Exception("变量" + mkey + "对应的值不能为空, key:" + key);
					}
				}
			}
			jobParam.put(key, tmp);
		}

		// 设置调试参数
		jobParam.put(MRConfiguration.INTERNAL_JOB_LOG_DEBUG, macroParam.get("debug"));
		jobParam.put(MRConfiguration.INTERNAL_JOB_LOG_DEBUG_ROWNUM, macroParam.get("rownum"));
		jobParam.put(MRConfiguration.MAPRED_JOB_QUEUE_NAME, macroParam.get("queue"));
		if (test) {
			jobParam.put(MRConfiguration.TEST_JOB_FLAG, "true");
		}

		// 校验部分参数
		if (tempNum > 1) {
			throw new Exception("变量-jobforce, -joblogid, -joblogtime, -jobfail，只能选择一个");
		}

		// 设置处理参数
		jobParam.put(FileConfiguration.INPUT_FILE_JOBLOG_TYPE, String.valueOf(joblogType));
		switch (joblogType) {
		case 1:
			jobParam.put(FileConfiguration.INPUT_FILE_FORCE_DO, String.valueOf(jobforce));
			break;
		case 2:
			jobParam.put(FileConfiguration.INPUT_FILE_JOBLOGID_DO, String.valueOf(joblogid));
			break;
		case 3:
			jobParam.put(FileConfiguration.INPUT_FILE_JOBLOGTIME_DO, String.valueOf(joblogtime));
			break;
		case 4:
			jobParam.put(FileConfiguration.INPUT_FILE_JOBFAIL, String.valueOf(jobfail));
			break;
		default:
			break;
		}

		// 下载、处理
		if ((downFTPId > 0 && jobId <= 0) || downFail) { // 只下载
			// 宏变量替换
			List<DBSourceRecordPO> dbValues = (List<DBSourceRecordPO>) SourceUtil.deserializable(jobParam
					.get(FTPConfiguration.FTP_DOWN_MR_MAPRED_PARAM));
			replaceFTPParam(dbValues, macroParam);
			jobParam.put(FTPConfiguration.FTP_DOWN_MR_MAPRED_PARAM, SourceUtil.serializable(dbValues));
			jobParam.put(MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE,
					MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN);
			jobParam.put(MRConfiguration.MAPRED_JOB_NAME,
					jobParam.get(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_NAME));
			jobParam.put(MRConfiguration.MAPRED_INPUT_DIR,
					jobParam.get(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_SYS_INPUTPATH));
			jobParam.put(MRConfiguration.MAPRED_MAP_TASKS,
					jobParam.get(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_TASK_NUMBER));
			jobParam.put(MRConfiguration.MAPRED_REDUCE_TASKS, "0");
			jobParam.put(MRConfiguration.MAPRED_JOB_PRIORITY,
					jobParam.get(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_TASK_PRIORITY));
			jobParam.put(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_FORCE_REPEAT,
					String.valueOf(downforce ? 0 : -1));
			jobParam.put(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_RERUN_DOWN_LOGID,
					String.valueOf(downFTPLogId));
			jobParam.put(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_FAIL, String.valueOf(downFail ? 0 : -1));

			// 设置运行数据源.
			jobParam.put(MRConfiguration.FS_DEFAULT_NAME,
					jobParam.remove(FTPConfiguration.MAPRED_DOWNFTP_FS_DEFAULT_NAME));
			jobParam.put(MRConfiguration.MAPRED_JOB_TRACKER,
					jobParam.remove(FTPConfiguration.MAPRED_DOWNFTP_JOB_TRACKER_NAME));
		} else if (downFTPId > 0 && jobId > 0) { // 下载并处理
			// 宏变量替换
			List<DBSourceRecordPO> dbValues = (List<DBSourceRecordPO>) SourceUtil.deserializable(jobParam
					.get(FTPConfiguration.FTP_DOWN_MR_MAPRED_PARAM));
			replaceFTPParam(dbValues, macroParam);
			jobParam.put(FTPConfiguration.FTP_DOWN_MR_MAPRED_PARAM, SourceUtil.serializable(dbValues));
			jobParam.put(MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE,
					MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO);
			jobParam.put(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_FORCE_REPEAT,
					String.valueOf(downforce ? 0 : -1));
			jobParam.put(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_RERUN_DOWN_LOGID,
					String.valueOf(downFTPLogId));
			jobParam.put(FTPConfiguration.DOWN_PREFIX + FTPConfiguration.COL_FAIL, String.valueOf(downFail ? 0 : -1));

			// 设置运行数据源.
			jobParam.put(MRConfiguration.FS_DEFAULT_NAME,
					jobParam.remove(FTPConfiguration.MAPRED_DOWNFTP_FS_DEFAULT_NAME));
			jobParam.put(MRConfiguration.MAPRED_JOB_TRACKER,
					jobParam.remove(FTPConfiguration.MAPRED_DOWNFTP_JOB_TRACKER_NAME));
		} else if (downFTPId < 0 && jobId > 0) { // 只处理
			jobParam.put(MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE,
					MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DO);

			// 设置运行数据源.
			jobParam.put(MRConfiguration.FS_DEFAULT_NAME, jobParam.remove(MRConfiguration.MAPRED_FS_DEFAULT_NAME));
			jobParam.put(MRConfiguration.MAPRED_JOB_TRACKER, jobParam.remove(MRConfiguration.MAPRED_JOB_TRACKER_NAME));
		}

		// 上传
		if (upFTPId > 0 || upFail) {
			// 宏变量替换
			List<DBSourceRecordPO> dbValues = (List<DBSourceRecordPO>) SourceUtil.deserializable(jobParam
					.get(FTPConfiguration.FTP_UP_MR_MAPRED_PARAM));
			replaceFTPParam(dbValues, macroParam);
			jobParam.put(FTPConfiguration.FTP_UP_MR_MAPRED_PARAM, SourceUtil.serializable(dbValues));
			jobParam.put(MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE, MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE_UP);
			jobParam.put(FTPConfiguration.UP_PREFIX + FTPConfiguration.COL_FORCE_REPEAT,
					String.valueOf(upforce ? 0 : -1));
			jobParam.put(FTPConfiguration.UP_PREFIX + FTPConfiguration.COL_FAIL, String.valueOf(upFail ? 0 : -1));
			jobParam.put(FTPConfiguration.UP_PREFIX + FTPConfiguration.COL_RERUN_DOWN_LOGID, String.valueOf(upFTPLogId));
			if ((jobId < 0 && downFTPId < 0) || upFail) {
				jobParam.put(MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE,
						MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE_UP);
				jobParam.put(MRConfiguration.MAPRED_JOB_NAME,
						jobParam.get(FTPConfiguration.UP_PREFIX + FTPConfiguration.COL_NAME));
				jobParam.put(MRConfiguration.MAPRED_INPUT_DIR,
						jobParam.get(FTPConfiguration.UP_PREFIX + FTPConfiguration.COL_SYS_INPUTPATH));
				jobParam.put(MRConfiguration.MAPRED_MAP_TASKS,
						jobParam.get(FTPConfiguration.UP_PREFIX + FTPConfiguration.COL_TASK_NUMBER));
				jobParam.put(MRConfiguration.MAPRED_REDUCE_TASKS, "0");
				jobParam.put(MRConfiguration.MAPRED_JOB_PRIORITY,
						jobParam.get(FTPConfiguration.UP_PREFIX + FTPConfiguration.COL_TASK_PRIORITY));
			}
		}

		// 设置FTP处理数据的格式类型
		if (downFTPId > 0 || upFTPId > 0 || downFail || upFail) {
			setFTPFormatType(jobParam);
		}

		// 替换设置宏变量
		for (String key : pmacroParam.keySet()) {
			jobParam.put(key, pmacroParam.get(key));
		}
		run(db, jobId, jobParam);
		System.exit(0);
	}

	private static void replaceFTPParam(List<DBSourceRecordPO> dbValues, Map<String, String> macroParam)
			throws Exception {
		for (DBSourceRecordPO dbsrp : dbValues) {
			dbsrp.setInputQuerySql(getRepleaseValue(macroParam, dbsrp.getInputQuerySql()));
			dbsrp.setInputPath(getRepleaseValue(macroParam, dbsrp.getInputPath()));
			dbsrp.setInputFileRule(getRepleaseValue(macroParam, dbsrp.getInputFileRule()));
			dbsrp.setInputMovePath(getRepleaseValue(macroParam, dbsrp.getInputMovePath()));
			dbsrp.setInputRename(getRepleaseValue(macroParam, dbsrp.getInputRename()));
			dbsrp.setInputRenameRule(getRepleaseValue(macroParam, dbsrp.getInputRenameRule()));
			dbsrp.setOutputMovePath(getRepleaseValue(macroParam, dbsrp.getOutputMovePath()));
			dbsrp.setOutputPath(getRepleaseValue(macroParam, dbsrp.getOutputPath()));
			dbsrp.setOutputRename(getRepleaseValue(macroParam, dbsrp.getOutputRename()));
			dbsrp.setOutputRenameRule(getRepleaseValue(macroParam, dbsrp.getOutputRenameRule()));
		}
	}

	private static String getRepleaseValue(Map<String, String> macroParam, String value) throws Exception {
		if (value == null || value.trim().length() <= 0) {
			return value;
		}

		if (value.indexOf("MOchild") > 0) {
			MRLog.systemOut(value);
		}
		String tmp = null;
		Iterator<String> miterator = macroParam.keySet().iterator();
		while (miterator.hasNext()) {
			String mkey = miterator.next();
			String mvalue = macroParam.get(mkey);
			try {
				tmp = value.replaceAll("\\{(?i)" + mkey + "\\}", mvalue);
			} catch (Exception e) {
				throw new Exception("变量" + mkey + "对应的值不能为空, value:" + mvalue);
			}

			if (!value.equals(tmp) && null == mvalue) {
				throw new Exception("变量" + mkey + "对应的值不能为空, value:" + mvalue);
			}

			value = tmp;
		}
		MRLog.systemOut(tmp);
		return tmp;
	}

	/**
	 * 执行方法
	 * 
	 * @param db
	 *            请求对象
	 * @param jobId
	 *            job id
	 * @param para
	 *            运行参数
	 * @throws Exception
	 */
	private static void run(DBRequestJob db, int jobId, Map<String, String> para) throws Exception {
		MRJOBClient client = new MRJOBClient();
		client.run(para);
	}

	/**
	 * 处理特殊参数(输入参数、输出参数、输入输出关系参数的存在换行符的情况)
	 * 
	 * @param jobParam
	 *            参数列表
	 */
	private static void special(Map<String, String> jobParam) {
		String tmp = jobParam.get(MRConfiguration.SYS_INPUT_FIELD_NAMES_PROPERTY);
		if (null != tmp) {
			jobParam.put(MRConfiguration.SYS_INPUT_FIELD_NAMES_PROPERTY, tmp.replaceAll("\r|\n", ""));
		}

		tmp = jobParam.get(MRConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY);
		if (null != tmp) {
			jobParam.put(MRConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY, tmp.replaceAll("\r|\n", ""));
		}

		tmp = jobParam.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
		if (null != tmp) {
			jobParam.put(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL, tmp.replaceAll("\r|\n", ""));
		}
	}

	/**
	 * 设置FTP处理数据的格式类型
	 * 
	 * @param jobParam
	 */
	private static void setFTPFormatType(Map<String, String> jobParam) {
		String intypes = jobParam.get(MRConfiguration.SYS_INPUT_FORMAT_TYPE);
		String outtypes = jobParam.get(MRConfiguration.SYS_OUTPUT_FORMAT_TYPE);
		jobParam.put(MRConfiguration.SYS_INPUT_FORMAT_TYPE, intypes == null ? "FTPDATA" : intypes + "," + "FTPDATA");
		jobParam.put(MRConfiguration.SYS_OUTPUT_FORMAT_TYPE, outtypes == null ? "FTPDATA" : outtypes + "," + "FTPDATA");
	}
}
