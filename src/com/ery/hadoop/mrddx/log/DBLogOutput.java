package com.ery.hadoop.mrddx.log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.ery.hadoop.mrddx.example.jdbc.DBRequestJob;
import com.ery.hadoop.mrddx.mode.MRFileListPO;
import com.ery.hadoop.mrddx.remote.Contant;
import com.ery.hadoop.mrddx.remote.FTPLogPO;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.base.support.jdbc.DataAccess;
import com.ery.base.support.jdbc.DataAccessFactory;
import com.ery.base.support.jdbc.DataSourceImpl;
import com.ery.base.support.jdbc.IParamsSetter;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.DataSourceManager;

/**
 * 日志信息输出到数据库
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-4
 * @version v1.0
 */
public class DBLogOutput implements ILogOutput {
	// 插入语句(MR_JOB_RUN_LOG)MYSQL
	private static final String SQL_INSERT_JOB_RUNLOG_MYSQL = "INSERT INTO MR_JOB_RUN_LOG(LOG_ID,JOB_ID,MONTH_NO,DATA_NO,START_DATE,ALL_FILE_SIZE,EXEC_CMD, QUEUE) VALUES(?,?,?,?,DATE_FORMAT(?,'%Y-%m-%d %T'),?,?,?)";
	// 插入语句(MR_JOB_RUN_LOG)ORACLE
	private static final String SQL_INSERT_JOB_RUNLOG_ORACLE = "INSERT INTO MR_JOB_RUN_LOG(LOG_ID,JOB_ID,MONTH_NO,DATA_NO,START_DATE,ALL_FILE_SIZE,EXEC_CMD, QUEUE) VALUES(?,?,?,?,to_date(?,'yyyy-MM-dd HH24:mi:ss'),?,?,?)";

	// 插入语句(MR_JOB_RUN_LOG_MSG)MYSQL
	private static final String SQL_INSERT_JOB_RUNLOG_MSG_MYSQL = "INSERT INTO MR_JOB_RUN_LOG_MSG(LOG_TIME, LOG_ID, LOG_TYPE, LOG_INFO) VALUES(DATE_FORMAT(?,'%Y-%m-%d %T'),?,?,?)";
	// 插入语句(MR_JOB_RUN_LOG_MSG)ORACLE
	private static final String SQL_INSERT_JOB_RUNLOG_MSG_ORACLE = "INSERT INTO MR_JOB_RUN_LOG_MSG(LOG_TIME, LOG_ID, LOG_TYPE, LOG_INFO) VALUES(to_date(?,'yyyy-MM-dd HH24:mi:ss'),?,?,?)";

	// 更新语句(MR_JOB_RUN_LOG)MYSQL 时间等信息
	private static final String SQL_UPDATE_JOB_RUNLOG_MYSQL = "UPDATE MR_JOB_RUN_LOG SET END_DATE=DATE_FORMAT(?,'%Y-%m-%d %T'), RUN_FLAG=?, ROW_RECORD=?, LOG_MSG=? where LOG_ID=?";
	// 更新语句(MR_JOB_RUN_LOG)ORACLE 时间等信息
	private static final String SQL_UPDATE_JOB_RUNLOG_ORACLE = "UPDATE MR_JOB_RUN_LOG SET END_DATE=to_date(?,'yyyy-MM-dd HH24:mi:ss'), RUN_FLAG=?, ROW_RECORD=?, LOG_MSG=? where LOG_ID=?";

	// 更新语句(MR_JOB_RUN_LOG)MYSQL map输入输出数据量信息
	private static final String SQL_UPDATE_JOB_RUNLOG_MAPCOUNT_MYSQL = "UPDATE MR_JOB_RUN_LOG SET MAP_INPUT_COUNT=?, MAP_OUTPUT_COUNT=?, INPUT_FILTER_COUNT=? where LOG_ID=?";
	// 更新语句(MR_JOB_RUN_LOG)ORACLE map输入输出数据量信息
	private static final String SQL_UPDATE_JOB_RUNLOG_MAPCOUNT_ORACLE = "UPDATE MR_JOB_RUN_LOG SET MAP_INPUT_COUNT=?,  MAP_OUTPUT_COUNT=?, INPUT_FILTER_COUNT=? where LOG_ID=?";

	// 更新语句(MR_JOB_RUN_LOG)MYSQL reduce输入输出数据量信息
	private static final String SQL_UPDATE_JOB_RUNLOG_REDUCECOUNT_MYSQL = "UPDATE MR_JOB_RUN_LOG SET REDUCE_INPUT_COUNT=?, REDUCE_OUTPUT_COUNT=?, OUTPUT_FILTER_COUNT=? where LOG_ID=?";
	// 更新语句(MR_JOB_RUN_LOG)ORACLE reduce输入输出数据量信息
	private static final String SQL_UPDATE_JOB_RUNLOG_REDUCECOUNT_ORACLE = "UPDATE MR_JOB_RUN_LOG SET REDUCE_INPUT_COUNT=?, REDUCE_OUTPUT_COUNT=?, OUTPUT_FILTER_COUNT=? where LOG_ID=?";

	// 更新语句(MR_JOB_RUN_LOG)文件大小
	private static final String SQL_UPDATE_JOB_RUNLOG_FILESIZE = "UPDATE MR_JOB_RUN_LOG SET ALL_FILE_SIZE=? where LOG_ID=?";

	// 插入语句(MR_JOB_MAP_RUN_LOG)MYSQL map运行日志
	private static final String SQL_INSERT_JOB_MAPRUNLOG_MYSQL = "INSERT INTO MR_JOB_MAP_RUN_LOG(MAP_TASK_ID,LOG_ID,START_DATE) VALUES(?,?,DATE_FORMAT(?,'%Y-%m-%d %T'))";
	// 插入语句(MR_JOB_MAP_RUN_LOG)ORACLE map运行日志
	private static final String SQL_INSERT_JOB_MAPRUNLOG_ORACLE = "INSERT INTO MR_JOB_MAP_RUN_LOG(MAP_TASK_ID,LOG_ID,START_DATE) VALUES(?,?,to_date(?,'yyyy-MM-dd HH24:mi:ss'))";

	// 查询语句(MR_JOB_MAP_RUN_LOG)MYSQL map运行日志
	private static final String SQL_QRY_JOB_MAPRUNLOG_MYSQL = "SELECT COUNT(1) COUNT FROM MR_JOB_MAP_RUN_LOG WHERE MAP_TASK_ID=?";
	// 查询语句(MR_JOB_MAP_RUN_LOG)ORACLE map运行日志
	private static final String SQL_QRY_JOB_MAPRUNLOG_ORACLE = "SELECT COUNT(1) COUNT FROM MR_JOB_MAP_RUN_LOG WHERE MAP_TASK_ID=?";

	// 查询语句(MR_JOB_MAP_RUN_LOG)MYSQL map运行日志
	private static final String SQL_QRY_JOB_REDUCERUNLOG_MYSQL = "SELECT COUNT(1) COUNT FROM MR_JOB_REDUCE_RUN_LOG WHERE REDUCE_TASK_ID=?";
	// 查询语句(MR_JOB_MAP_RUN_LOG)ORACLE map运行日志
	private static final String SQL_QRY_JOB_REDUCERUNLOG_ORACLE = "SELECT COUNT(1) COUNT FROM MR_JOB_REDUCE_RUN_LOG WHERE REDUCE_TASK_ID=?";

	// 更新语句(MR_JOB_MAP_RUN_LOG)MYSQL map运行日志
	private static final String SQL_UPDATE_JOB_MAPRUNLOG_MYSQL = "UPDATE MR_JOB_MAP_RUN_LOG SET MAP_INPUT_COUNT=?, MAP_OUTPUT_COUNT=?, END_DATE=DATE_FORMAT(?,'%Y-%m-%d %T'), RUN_FLAG=?, LOG_MSG=?, FILTER_COUNT=? where MAP_TASK_ID=?";
	// 更新语句(MR_JOB_MAP_RUN_LOG)ORACLE map运行日志
	private static final String SQL_UPDATE_JOB_MAPRUNLOG_ORACLE = "UPDATE MR_JOB_MAP_RUN_LOG SET MAP_INPUT_COUNT=?, MAP_OUTPUT_COUNT=?, END_DATE=to_date(?,'yyyy-MM-dd HH24:mi:ss'), RUN_FLAG=?, LOG_MSG=?, FILTER_COUNT=? where MAP_TASK_ID=?";

	// 插入语句(MR_JOB_REDUCE_RUN_LOG)MYSQL reduce运行日志
	private static final String SQL_INSERT_JOB_REDUCERUNLOG_MYSQL = "INSERT INTO MR_JOB_REDUCE_RUN_LOG(REDUCE_TASK_ID,LOG_ID,START_DATE) VALUES(?,?,DATE_FORMAT(?,'%Y-%m-%d %T'))";
	// 插入语句(MR_JOB_REDUCE_RUN_LOG)ORACLE reduce运行日志
	private static final String SQL_INSERT_JOB_REDUCERUNLOG_ORACLE = "INSERT INTO MR_JOB_REDUCE_RUN_LOG(REDUCE_TASK_ID,LOG_ID,START_DATE) VALUES(?,?,to_date(?,'yyyy-MM-dd HH24:mi:ss'))";

	// 更新语句(MR_JOB_REDUCE_RUN_LOG)MYSQL reduce运行日志
	private static final String SQL_UPDATE_JOB_REDUCERUNLOG_MYSQL = "UPDATE MR_JOB_REDUCE_RUN_LOG SET REDUCE_INPUT_COUNT=?, REDUCE_OUTPUT_COUNT=?, END_DATE=DATE_FORMAT(?,'%Y-%m-%d %T'), RUN_FLAG=?, LOG_MSG=?, FILTER_COUNT=? where REDUCE_TASK_ID=?";
	// 更新语句(MR_JOB_REDUCE_RUN_LOG)ORACLE reduce运行日志
	private static final String SQL_UPDATE_JOB_REDUCERUNLOG_ORACLE = "UPDATE MR_JOB_REDUCE_RUN_LOG SET REDUCE_INPUT_COUNT=?, REDUCE_OUTPUT_COUNT=?, END_DATE=to_date(?,'yyyy-MM-dd HH24:mi:ss'), RUN_FLAG=?, LOG_MSG=?, FILTER_COUNT=? where REDUCE_TASK_ID=?";

	// 插入语句(MR_JOB_MAP_RUN_LOG_MSG)MYSQL map运行日志详情
	private static final String SQL_INSERT_JOB_MAPRUNLOG_MSG_MYSQL = "INSERT INTO MR_JOB_MAP_RUN_LOG_MSG(MAP_TASK_ID,LOG_TYPE,LOG_DATE,LOG_MSG) VALUES(?,?,DATE_FORMAT(?,'%Y-%m-%d %T'),?)";
	// 插入语句(MR_JOB_MAP_RUN_LOG_MSG)ORACLE map运行日志详情
	private static final String SQL_INSERT_JOB_MAPRUNLOG_MSG_ORCALE = "INSERT INTO MR_JOB_MAP_RUN_LOG_MSG(LOG_ID, MAP_TASK_ID,LOG_TYPE,LOG_DATE,LOG_MSG) VALUES(?,?,?,to_date(?,'yyyy-MM-dd HH24:mi:ss'),?)";

	// 插入语句(MR_JOB_REDUCE_RUN_LOG_MSG)MYSQL map运行日志详情
	private static final String SQL_INSERT_JOB_REDUCERUNLOG_MSG_MYSQL = "INSERT INTO MR_JOB_REDUCE_RUN_LOG_MSG(REDUCE_TASK_ID,LOG_TYPE,LOG_DATE,LOG_MSG) VALUES(?,?,DATE_FORMAT(?,'%Y-%m-%d %T'),?)";
	// 插入语句(MR_JOB_REDUCE_RUN_LOG_MSG)ORACLE map运行日志详情
	private static final String SQL_INSERT_JOB_REDUCERUNLOG_MSG_ORCALE = "INSERT INTO MR_JOB_REDUCE_RUN_LOG_MSG(LOG_ID, REDUCE_TASK_ID,LOG_TYPE,LOG_DATE,LOG_MSG) VALUES(?,?,?,to_date(?,'yyyy-MM-dd HH24:mi:ss'),?)";

	// map日志id序列
	private static final String SEQUENCE_INSERT_JOB_MAPRUNLOG_MSG_ORCALE = "SELECT MR_JOB_MAP_RUN_LOG_MSG_ID.NEXTVAL FROM DUAL";
	// reduce日志id序列
	private static final String SEQUENCE_INSERT_JOB_REDUCERUNLOG_MSG_ORCALE = "SELECT MR_JOB_REDUCE_RUN_LOG_MSG_ID.NEXTVAL FROM DUAL";

	// 查询map的输入输出记录数据
	private static final String SQL_SELECT_JOB_RUNLOG_MAPCOUNT = "SELECT MAP_INPUT_COUNT, MAP_OUTPUT_COUNT, INPUT_FILTER_COUNT FROM MR_JOB_RUN_LOG where LOG_ID=?";
	// 查询reduce的输入输出记录数据
	private static final String SQL_SELECT_JOB_RUNLOG_REDUCECOUNT = "SELECT REDUCE_INPUT_COUNT, REDUCE_OUTPUT_COUNT, OUTPUT_FILTER_COUNT FROM MR_JOB_RUN_LOG where LOG_ID=?";

	// 插入语句(MR_FTP_COL_REMOTE_FILE_LOG_MSG)MYSQL
	private static final String SQL_INSERT_JOB_RUN_REMOTE_FILE_LOG_MYSQL = "INSERT INTO MR_FTP_COL_REMOTE_FILE_LOG_MSG(COL_ID, COL_LOG_ID, INPUT_FILE_LASTMODIFY_DATE, STATUS, INPUT_FILE_MSG, OUTPUT_FILE_MSG, RUN_DATE, COL_DETAIL_LOG_ID) VALUES(?,?,?,?,?,?,?,?)";
	// 插入语句(MR_FTP_COL_REMOTE_FILE_LOG_MSG)ORACLE
	private static final String SQL_INSERT_JOB_RUN_REMOTE_FILE_LOG_ORACLE = "INSERT INTO MR_FTP_COL_REMOTE_FILE_LOG_MSG(COL_ID, COL_LOG_ID, INPUT_FILE_LASTMODIFY_DATE, STATUS, INPUT_FILE_MSG, OUTPUT_FILE_MSG, RUN_DATE, COL_DETAIL_LOG_ID) VALUES(?,?,?,?,?,?,?,?)";
	// 更新语句(MR_FTP_COL_REMOTE_FILE_LOG_MSG)MYSQL
	private static final String SQL_UPDATE_JOB_RUN_REMOTE_FILE_LOG_MYSQL = "UPDATE MR_FTP_COL_REMOTE_FILE_LOG_MSG SET INPUT_FILE_LASTMODIFY_DATE=?, STATUS=?, RUN_DATE=?, COL_DETAIL_LOG_ID=?, FILTER_COUNT=? where COL_ID=? AND INPUT_FILE_MSG=? AND OUTPUT_FILE_MSG=? AND COL_LOG_ID=?";
	// 更新语句(MR_FTP_COL_REMOTE_FILE_LOG_MSG)ORACLE
	private static final String SQL_UPDATE_JOB_RUN_REMOTE_FILE_LOG_ORACLE = "UPDATE MR_FTP_COL_REMOTE_FILE_LOG_MSG SET INPUT_FILE_LASTMODIFY_DATE=?, STATUS=?, RUN_DATE=?, COL_DETAIL_LOG_ID=?, FILTER_COUNT=? where COL_ID=? AND INPUT_FILE_MSG=? AND OUTPUT_FILE_MSG=? AND COL_LOG_ID=?";
	// 查询语句()
	private static final String SQL_SELECT_JOB_RUN_REMOTE_FILE_LOG = "SELECT INPUT_FILE_LASTMODIFY_DATE, STATUS, COL_DETAIL_LOG_ID FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG where COL_ID=? AND INPUT_FILE_MSG=? AND OUTPUT_FILE_MSG=? AND COL_LOG_ID=?";
	// 查询执行之后的日志信息
	private static final String SQL_SELECT_JOB_RUN_REMOTE_DETAIL_FILE_LOG_ORACLE = "SELECT A.INPUT_FILE_LASTMODIFY_DATE, A.STATUS,  A.COL_DETAIL_LOG_ID,  B.STATUS DETAILS_STATUS,	 B.IS_OUTPUT_RENAME, B.IS_MOVE_OUTPUT, B.OUTPUT_RENAME_STATUS, B.INPUT_RENAME_STATUS, B.MOVE_OUTPUT_STATUS,	 B.MOVE_INPUT_STATUS, B.IS_DOINPUTFILETYPE,	 B.DELETE_INPUT_STATUS  FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG A INNER JOIN MR_FTP_COL_DETAIL_FILELOG B ON A.COL_DETAIL_LOG_ID = B.ID WHERE A.COL_ID = ? AND A.INPUT_FILE_MSG = ?  AND A.OUTPUT_FILE_MSG = ? AND A.COL_LOG_ID =? ";
	private static final String SQL_SELECT_JOB_RUN_REMOTE_DETAIL_FILE_LOG_MYSQL = "SELECT A.INPUT_FILE_LASTMODIFY_DATE, A.STATUS,  A.COL_DETAIL_LOG_ID,  B.STATUS DETAILS_STATUS,	 B.IS_OUTPUT_RENAME, B.IS_MOVE_OUTPUT, B.OUTPUT_RENAME_STATUS, B.INPUT_RENAME_STATUS, B.MOVE_OUTPUT_STATUS,	 B.MOVE_INPUT_STATUS, B.IS_DOINPUTFILETYPE,	 B.DELETE_INPUT_STATUS  FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG A INNER JOIN MR_FTP_COL_DETAIL_FILELOG B ON A.COL_DETAIL_LOG_ID = B.ID WHERE A.COL_ID = ? AND A.INPUT_FILE_MSG = ?  AND A.OUTPUT_FILE_MSG = ? AND A.COL_LOG_ID =? ";

	// 查询采集的最新日志ID
	private static final String SQL_SELECT_JOB_RUN_COL_LAST_LOGID_ORACLE = "SELECT MAX(COL_LOG_ID) FROM MR_FTP_COL_FILE_LOG WHERE COL_ID = ?";
	private static final String SQL_SELECT_JOB_RUN_COL_LAST_LOGID_MYSQL = "SELECT MAX(COL_LOG_ID) FROM MR_FTP_COL_FILE_LOG WHERE COL_ID = ?";

	// 查询详细日志信息
	private static final String SQL_SELECT_DETAIL_FILE_LOG_MYSQL = "SELECT ID,STATUS,INPUT_RENAME_STATUS,MOVE_INPUT_STATUS,IS_DOINPUTFILETYPE,DELETE_INPUT_STATUS,MOVE_OUTPUT_STATUS,OUTPUT_RENAME_STATUS,INPUT_FILE_NAME,OUTPUT_FILE_NAME,INPUT_PATH,OUTPUT_PATH,MOVE_OUTPUT_PATH,MOVE_INPUT_PATH,INPUT_RENAME,OUTPUT_RENAME FROM MR_FTP_COL_DETAIL_FILELOG WHERE ID =?";
	private static final String SQL_SELECT_DETAIL_FILE_LOG_ORACLE = "SELECT ID,STATUS,INPUT_RENAME_STATUS,MOVE_INPUT_STATUS,IS_DOINPUTFILETYPE,DELETE_INPUT_STATUS,MOVE_OUTPUT_STATUS,OUTPUT_RENAME_STATUS,INPUT_FILE_NAME,OUTPUT_FILE_NAME,INPUT_PATH,OUTPUT_PATH,MOVE_OUTPUT_PATH,MOVE_INPUT_PATH,INPUT_RENAME,OUTPUT_RENAME FROM MR_FTP_COL_DETAIL_FILELOG WHERE ID = ?";

	// 记录上传或下载的文件日志
	private static final String SEQUENCE_FTP_FILE_LOG_ORCALE = "COL_SEQ_FILELOG_FILE_ID";
	private static final String SQL_FTP_FILE_LOG_MYSQL = "INSERT INTO MR_FTP_COL_FILE_LOG(COL_LOG_ID, COL_ID, START_TIME, FILE_NUM, FILE_TOTALSIZE, STATUS, QUEUE, EXEC_CMD) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_FTP_FILE_LOG_ORACLE = "INSERT INTO MR_FTP_COL_FILE_LOG(COL_LOG_ID, COL_ID, START_TIME, FILE_NUM, FILE_TOTALSIZE, STATUS, QUEUE, EXEC_CMD) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String SQL_FTP_FILE_LOG_UPDATE_MYSQL = "UPDATE MR_FTP_COL_FILE_LOG SET END_TIME = ? , STATUS = ?, FILE_NUM=?, FILE_TOTALSIZE=? WHERE COL_LOG_ID = ?";
	private static final String SQL_FTP_FILE_LOG_UPDATE_ORACLE = "UPDATE MR_FTP_COL_FILE_LOG SET END_TIME = ? , STATUS = ?, FILE_NUM=?, FILE_TOTALSIZE=? WHERE COL_LOG_ID = ?";
	private static final String SQL_FTP_FILE_LOG_UPDATE_RERUN_MYSQL = "UPDATE MR_FTP_COL_FILE_LOG SET START_TIME = ? , STATUS = ? WHERE COL_LOG_ID = ?";
	private static final String SQL_FTP_FILE_LOG_UPDATE_RERUN_ORACLE = "UPDATE MR_FTP_COL_FILE_LOG SET START_TIME = ? , STATUS = ? WHERE COL_LOG_ID = ?";
	private static final String SQL_FTP_FILE_LOG_SELECT_FILENUM = "SELECT count(*) FILE_COUNT, SUM(FILE_SIZE) TOTAL_FILE_SIZE FROM (SELECT distinct (A.INPUT_FILE_MSG), B.FILE_SIZE FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG A, MR_FTP_COL_DETAIL_FILELOG B WHERE A.COL_LOG_ID = B.COL_LOG_ID AND B.ID = A.COL_DETAIL_LOG_ID  AND A.COL_LOG_ID = ?)";

	private static final String SQL_FTP_FILE_LOG_STATUS_MYSQL = "SELECT COUNT(0) COUNT FROM MR_FTP_COL_DETAIL_FILELOG b WHERE col_log_id = ? AND status IN (0,2) AND (b.input_file_name,b.input_path,b.id) IN (SELECT a.input_file_name,a.input_path,MAX(a.id) FROM MR_FTP_COL_DETAIL_FILELOG  a WHERE  col_log_id = ? GROUP BY a.input_file_name,a.input_path)";
	private static final String SQL_FTP_FILE_LOG_STATUS_ORACLE = "SELECT COUNT(0) COUNT FROM MR_FTP_COL_DETAIL_FILELOG b WHERE col_log_id = ? AND status IN (0,2) AND (b.input_file_name,b.input_path,b.id) IN (SELECT a.input_file_name,a.input_path,MAX(a.id) FROM MR_FTP_COL_DETAIL_FILELOG  a WHERE  col_log_id = ? GROUP BY a.input_file_name,a.input_path)";

	private static final String SQL_FTP_FILE_LOG_RESULT_MYSQL = "SELECT * FROM (SELECT COUNT(0) FAIL_COUNT FROM MR_FTP_COL_DETAIL_FILELOG t WHERE STATUS IN (0, 2) AND COL_LOG_ID = ?) A, (SELECT COUNT(0) SUCC_COUNT FROM MR_FTP_COL_DETAIL_FILELOG t WHERE STATUS NOT IN (0, 2) AND COL_LOG_ID = ?) B";
	private static final String SQL_FTP_FILE_LOG_RESULT_ORACLE = "SELECT * FROM (SELECT COUNT(0) FAIL_COUNT FROM MR_FTP_COL_DETAIL_FILELOG t WHERE STATUS IN (0, 2) AND COL_LOG_ID = ?) A, (SELECT COUNT(0) SUCC_COUNT FROM MR_FTP_COL_DETAIL_FILELOG t WHERE STATUS NOT IN (0, 2) AND COL_LOG_ID = ?) B";

	// 记录上传或下载的文件详细日志
	private static final String SEQUENCE_FTP_FILE_DETAIL_LOG_ORCALE = "COL_SEQ_DETAIL_FILELOG_ID";
	private static final String SQL_FTP_FILE_DETAIL_LOG_MYSQL = "INSERT INTO MR_FTP_COL_DETAIL_FILELOG (ID,COL_LOG_ID,COL_ID,START_TIME,INPUT_FILE_NAME,INPUT_PATH,OUTPUT_PATH,FILE_SIZE,STATUS,IS_OUTPUT_RENAME,IS_MOVE_OUTPUT,MOVE_OUTPUT_PATH,IS_DOINPUTFILETYPE,MOVE_INPUT_PATH, INPUT_RENAME, OUTPUT_RENAME) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String SQL_FTP_FILE_DETAIL_LOG_ORACLE = "INSERT INTO MR_FTP_COL_DETAIL_FILELOG (ID,COL_LOG_ID,COL_ID,START_TIME,INPUT_FILE_NAME,INPUT_PATH,OUTPUT_PATH,FILE_SIZE,STATUS,IS_OUTPUT_RENAME,IS_MOVE_OUTPUT,MOVE_OUTPUT_PATH,IS_DOINPUTFILETYPE,MOVE_INPUT_PATH, INPUT_RENAME, OUTPUT_RENAME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String SQL_FTP_FILE_DETAIL_LOG_UPDATE_MYSQL = "UPDATE MR_FTP_COL_DETAIL_FILELOG SET END_TIME=?, DELETE_INPUT_STATUS=?, MOVE_INPUT_STATUS=?,MOVE_OUTPUT_STATUS=?,OUTPUT_FILE_NAME=?,OUTPUT_RENAME_STATUS=? ,STATUS=?, FILE_ID=?,INPUT_RENAME_STATUS=? WHERE ID=?";
	private static final String SQL_FTP_FILE_DETAIL_LOG_UPDATE_ORACLE = "UPDATE MR_FTP_COL_DETAIL_FILELOG SET END_TIME=?, DELETE_INPUT_STATUS=?, MOVE_INPUT_STATUS=?,MOVE_OUTPUT_STATUS=?,OUTPUT_FILE_NAME=?,OUTPUT_RENAME_STATUS=? ,STATUS=?, FILE_ID=?,INPUT_RENAME_STATUS=? WHERE ID=?";

	// 记录上传或下载的文件错误日志
	private static final String SEQUENCE_FTP_FILE_ERROR_LOG_ORCALE = "COL_SEQ_FILE_ERROR_LOG_ID";
	private static final String SQL_FTP_FILE_ERROR_LOG_MYSQL = "INSERT INTO MR_FTP_COL_FILE_ERROR_LOG(ID,COL_LOG_ID,CREATE_TIME,PROTOCOL,IP,PORT,USERNAME,ROOTPATH,TYPE,MSG) VALUES (?,?,?,?,?,?,?,?,?,?)";
	private static final String SQL_FTP_FILE_ERROR_LOG_ORACLE = "INSERT INTO MR_FTP_COL_FILE_ERROR_LOG(ID,COL_LOG_ID,CREATE_TIME,PROTOCOL,IP,PORT,USERNAME,ROOTPATH,TYPE,MSG) VALUES (?,?,?,?,?,?,?,?,?,?)";
	private static final String SQL_FTP_FILE_ERROR_LOG_UPDATE_MYSQL = "INSERT INTO MR_FTP_COL_FILE_ERROR_LOG(ID,COL_LOG_ID,CREATE_TIME,PROTOCOL,IP,PORT,USERNAME,ROOTPATH,TYPE,MSG) VALUES (?,?,?,?,?,?,?)";
	private static final String SQL_FTP_FILE_ERROR_LOG_UPDATE_ORACLE = "INSERT INTO MR_FTP_COL_FILE_ERROR_LOG(ID,COL_LOG_ID,CREATE_TIME,PROTOCOL,IP,PORT,USERNAME,ROOTPATH,TYPE,MSG) VALUES (?,?,?,?,?,?,?)";

	// 查询col_id下某次col_log_id失败的任务列表
	private static final String SQL_FTP_COL_ID_FAIL_LIST_MYSQL = "SELECT A.COL_LOG_ID,"
			+ "       A.INPUT_FILE_LASTMODIFY_DATE, "
			+ "       A.STATUS,                     "
			+ "       A.INPUT_FILE_MSG,             "
			+ "       A.OUTPUT_FILE_MSG,            "
			+ "       A.COL_DETAIL_LOG_ID,          "
			+ "       B.COL_ID,                     "
			+ "       B.INPUT_FILE_NAME,            "
			+ "       B.OUTPUT_FILE_NAME,           "
			+ "       B.INPUT_PATH,                 "
			+ "       B.OUTPUT_PATH,                "
			+ "       B.FILE_SIZE,                  "
			+ "       B.STATUS DETAILS_STATUS,      "
			+ "       B.IS_OUTPUT_RENAME,           "
			+ "       B.OUTPUT_RENAME_STATUS,       "
			+ "       B.IS_MOVE_OUTPUT,             "
			+ "       B.MOVE_OUTPUT_PATH,           "
			+ "       B.MOVE_OUTPUT_STATUS,         "
			+ "       B.IS_DOINPUTFILETYPE,         "
			+ "       B.DELETE_INPUT_STATUS,        "
			+ "       B.MOVE_INPUT_PATH,            "
			+ "       B.MOVE_INPUT_STATUS,          "
			+ "       B.FILE_ID,                    "
			+ "       B.INPUT_RENAME,               "
			+ "       B.INPUT_RENAME_STATUS,        "
			+ "       B.OUTPUT_RENAME               "
			+ "  FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG A, MR_FTP_COL_DETAIL_FILELOG B "
			+ " WHERE A.COL_DETAIL_LOG_ID = B.ID "
			+ "   AND A.COL_ID = ? "
			+ "   AND A.COL_DETAIL_LOG_ID IN( "
			+ "   SELECT MAX(C.COL_DETAIL_LOG_ID) COL_DETAIL_LOG_ID FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG C WHERE C.COL_ID = ? AND C.STATUS=2 GROUP BY C.INPUT_FILE_MSG , C.OUTPUT_FILE_MSG "
			+ "   UNION "
			+ "   SELECT MAX(D.ID) FROM MR_FTP_COL_DETAIL_FILELOG D  WHERE D.COL_ID = ? AND D.STATUS=2 GROUP BY D.INPUT_PATH,D.INPUT_FILE_NAME,D.OUTPUT_PATH,D.OUTPUT_FILE_NAME) ";

	// 查询col_id下某次col_log_id失败的任务列表
	private static final String SQL_FTP_COL_ID_FAIL_LIST_ORACLE = "SELECT A.COL_LOG_ID,"
			+ "       A.INPUT_FILE_LASTMODIFY_DATE, "
			+ "       A.STATUS,                     "
			+ "       A.INPUT_FILE_MSG,             "
			+ "       A.OUTPUT_FILE_MSG,            "
			+ "       A.COL_DETAIL_LOG_ID,          "
			+ "       B.COL_ID,                     "
			+ "       B.INPUT_FILE_NAME,            "
			+ "       B.OUTPUT_FILE_NAME,           "
			+ "       B.INPUT_PATH,                 "
			+ "       B.OUTPUT_PATH,                "
			+ "       B.FILE_SIZE,                  "
			+ "       B.STATUS DETAILS_STATUS,      "
			+ "       B.IS_OUTPUT_RENAME,           "
			+ "       B.OUTPUT_RENAME_STATUS,       "
			+ "       B.IS_MOVE_OUTPUT,             "
			+ "       B.MOVE_OUTPUT_PATH,           "
			+ "       B.MOVE_OUTPUT_STATUS,         "
			+ "       B.IS_DOINPUTFILETYPE,         "
			+ "       B.DELETE_INPUT_STATUS,        "
			+ "       B.MOVE_INPUT_PATH,            "
			+ "       B.MOVE_INPUT_STATUS,          "
			+ "       B.FILE_ID,                    "
			+ "       B.INPUT_RENAME,               "
			+ "       B.INPUT_RENAME_STATUS,        "
			+ "       B.OUTPUT_RENAME               "
			+ "  FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG A, MR_FTP_COL_DETAIL_FILELOG B "
			+ " WHERE A.COL_DETAIL_LOG_ID = B.ID "
			+ "   AND A.COL_ID = ? "
			+ "   AND A.COL_DETAIL_LOG_ID IN( "
			+ "   SELECT MAX(C.COL_DETAIL_LOG_ID) COL_DETAIL_LOG_ID FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG C WHERE C.COL_ID = ? AND C.STATUS=2 GROUP BY C.INPUT_FILE_MSG , C.OUTPUT_FILE_MSG "
			+ "   UNION "
			+ "   SELECT MAX(D.ID) FROM MR_FTP_COL_DETAIL_FILELOG D  WHERE D.COL_ID = ? AND D.STATUS=2 GROUP BY D.INPUT_PATH,D.INPUT_FILE_NAME,D.OUTPUT_PATH,D.OUTPUT_FILE_NAME) ";

	// 查询col_id下某次col_log_id失败的任务列表
	private static final String SQL_FTP_COL_LOG_ID_FAIL_LIST_MYSQL = "SELECT A.COL_LOG_ID,"
			+ "       A.INPUT_FILE_LASTMODIFY_DATE, "
			+ "       A.STATUS,                     "
			+ "       A.INPUT_FILE_MSG,             "
			+ "       A.OUTPUT_FILE_MSG,            "
			+ "       A.COL_DETAIL_LOG_ID,          "
			+ "       B.COL_ID,                     "
			+ "       B.INPUT_FILE_NAME,            "
			+ "       B.OUTPUT_FILE_NAME,           "
			+ "       B.INPUT_PATH,                 "
			+ "       B.OUTPUT_PATH,                "
			+ "       B.FILE_SIZE,                  "
			+ "       B.STATUS DETAILS_STATUS,      "
			+ "       B.IS_OUTPUT_RENAME,           "
			+ "       B.OUTPUT_RENAME_STATUS,       "
			+ "       B.IS_MOVE_OUTPUT,             "
			+ "       B.MOVE_OUTPUT_PATH,           "
			+ "       B.MOVE_OUTPUT_STATUS,         "
			+ "       B.IS_DOINPUTFILETYPE,         "
			+ "       B.DELETE_INPUT_STATUS,        "
			+ "       B.MOVE_INPUT_PATH,            "
			+ "       B.MOVE_INPUT_STATUS,          "
			+ "       B.FILE_ID,                    "
			+ "       B.INPUT_RENAME,               "
			+ "       B.INPUT_RENAME_STATUS,        "
			+ "       B.OUTPUT_RENAME               "
			+ "  FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG A, MR_FTP_COL_DETAIL_FILELOG B "
			+ " WHERE A.COL_DETAIL_LOG_ID = B.ID "
			+ "   AND A.COL_ID = ? "
			+ "   AND A.COL_DETAIL_LOG_ID IN( "
			+ "   SELECT MAX(C.COL_DETAIL_LOG_ID) COL_DETAIL_LOG_ID FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG C WHERE C.COL_ID = ? AND C.COL_LOG_ID=?  AND C.STATUS=2 GROUP BY C.INPUT_FILE_MSG , C.OUTPUT_FILE_MSG "
			+ "   UNION "
			+ "   SELECT MAX(D.ID) FROM MR_FTP_COL_DETAIL_FILELOG D  WHERE D.COL_ID = ? AND D.COL_LOG_ID = ? AND D.STATUS=2 GROUP BY D.INPUT_PATH,D.INPUT_FILE_NAME,D.OUTPUT_PATH,D.OUTPUT_FILE_NAME) ";

	// 查询col_id下某次col_log_id失败的任务列表
	private static final String SQL_FTP_COL_LOG_ID_FAIL_LIST_ORACLE = "SELECT A.COL_LOG_ID,"
			+ "       A.INPUT_FILE_LASTMODIFY_DATE, "
			+ "       A.STATUS,                     "
			+ "       A.INPUT_FILE_MSG,             "
			+ "       A.OUTPUT_FILE_MSG,            "
			+ "       A.COL_DETAIL_LOG_ID,          "
			+ "       B.COL_ID,                     "
			+ "       B.INPUT_FILE_NAME,            "
			+ "       B.OUTPUT_FILE_NAME,           "
			+ "       B.INPUT_PATH,                 "
			+ "       B.OUTPUT_PATH,                "
			+ "       B.FILE_SIZE,                  "
			+ "       B.STATUS DETAILS_STATUS,      "
			+ "       B.IS_OUTPUT_RENAME,           "
			+ "       B.OUTPUT_RENAME_STATUS,       "
			+ "       B.IS_MOVE_OUTPUT,             "
			+ "       B.MOVE_OUTPUT_PATH,           "
			+ "       B.MOVE_OUTPUT_STATUS,         "
			+ "       B.IS_DOINPUTFILETYPE,         "
			+ "       B.DELETE_INPUT_STATUS,        "
			+ "       B.MOVE_INPUT_PATH,            "
			+ "       B.MOVE_INPUT_STATUS,          "
			+ "       B.FILE_ID,                    "
			+ "       B.INPUT_RENAME,               "
			+ "       B.INPUT_RENAME_STATUS,        "
			+ "       B.OUTPUT_RENAME               "
			+ "  FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG A, MR_FTP_COL_DETAIL_FILELOG B "
			+ " WHERE A.COL_DETAIL_LOG_ID = B.ID "
			+ "   AND A.COL_ID = ? "
			+ "   AND A.COL_DETAIL_LOG_ID IN( "
			+ "   SELECT MAX(C.COL_DETAIL_LOG_ID) COL_DETAIL_LOG_ID FROM MR_FTP_COL_REMOTE_FILE_LOG_MSG C WHERE C.COL_ID = ? AND C.COL_LOG_ID=?  AND C.STATUS=2 GROUP BY C.INPUT_FILE_MSG , C.OUTPUT_FILE_MSG "
			+ "   UNION "
			+ "   SELECT MAX(D.ID) FROM MR_FTP_COL_DETAIL_FILELOG D  WHERE D.COL_ID = ? AND D.COL_LOG_ID = ? AND D.STATUS=2 GROUP BY D.INPUT_PATH,D.INPUT_FILE_NAME,D.OUTPUT_PATH,D.OUTPUT_FILE_NAME) ";

	// 记录map的处理
	private static final String SEQUENCE_MR_JOB_MAP_DATALOG_ORCALE = "Col_SEQ_MR_JOB_MAP_DATALOG_ID";
	private static final String SQL_MR_JOB_MAP_DATALOG_MYSQL = "INSERT INTO MR_JOB_MAP_DATALOG(ID,JOBID,FILE_ID,DATA_TYPE,FILE_PATH,FILE_SIZE,START_TIME,STATUS,JOB_LOG_ID, DATA_SOURCE_ID) VALUES(?,?,?,?,?,?,?,?,?,?)";
	private static final String SQL_MR_JOB_MAP_DATALOG_ORCALE = "INSERT INTO MR_JOB_MAP_DATALOG(ID,JOBID,FILE_ID,DATA_TYPE,FILE_PATH,FILE_SIZE,START_TIME,STATUS,JOB_LOG_ID, DATA_SOURCE_ID) VALUES(?,?,?,?,?,?,?,?,?,?)";
	private static final String SQL_MR_JOB_MAP_DATALOG_MYSQL_FTP = "INSERT INTO MR_JOB_MAP_DATALOG(ID,JOBID,FILE_ID,FILE_PATH,FILE_SIZE,START_TIME,STATUS,JOB_LOG_ID, DATA_SOURCE_ID) VALUES(?,?,?,?,?,?,?,?,?)";
	private static final String SQL_MR_JOB_MAP_DATALOG_ORCALE_FTP = "INSERT INTO MR_JOB_MAP_DATALOG(ID,JOBID,FILE_ID,FILE_PATH,FILE_SIZE,START_TIME,STATUS,JOB_LOG_ID, DATA_SOURCE_ID) VALUES(?,?,?,?,?,?,?,?,?)";
	private static final String SQL_MR_JOB_MAP_DATALOG_UPDATE_MYSQL = "UPDATE MR_JOB_MAP_DATALOG SET END_TIME=?, TOTAL_COUNT=?, SUCCESS_COUNT=?, FAIL_COUNT=?, STATUS=?, ERROR_PATH=? WHERE ID=?";
	private static final String SQL_MR_JOB_MAP_DATALOG_UPDATE_ORCALE = "UPDATE MR_JOB_MAP_DATALOG SET END_TIME=?, TOTAL_COUNT=?, SUCCESS_COUNT=?, FAIL_COUNT=?, STATUS=?, ERROR_PATH=? WHERE ID=?";
	private static final String SQL_MR_JOB_MAP_DATALOG_UPDATE_MYSQL_FTP = "UPDATE MR_JOB_MAP_DATALOG SET END_TIME=?, TOTAL_COUNT=?, SUCCESS_COUNT=?, FAIL_COUNT=?, STATUS=?, ERROR_PATH=?, DATA_TYPE=? WHERE ID=?";
	private static final String SQL_MR_JOB_MAP_DATALOG_UPDATE_ORCALE_FTP = "UPDATE MR_JOB_MAP_DATALOG SET END_TIME=?, TOTAL_COUNT=?, SUCCESS_COUNT=?, FAIL_COUNT=?, STATUS=?, ERROR_PATH=?, DATA_TYPE=?  WHERE ID=?";
	private static final String SQL_MR_JOB_MAP_DATALOG_UPDATE_MYSQL_STATUS = "UPDATE MR_JOB_MAP_DATALOG SET STATUS=? WHERE ID=?";
	private static final String SQL_MR_JOB_MAP_DATALOG_UPDATE_ORCALE_STATUS = "UPDATE MR_JOB_MAP_DATALOG SET STATUS=? WHERE ID=?";

	private static final String SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY = "SELECT MAX(ID) ID, STATUS FROM MR_JOB_MAP_DATALOG  WHERE FILE_ID=? AND JOBID= ? GROUP BY STATUS";
	private static final String SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY = "SELECT MAX(ID) ID, STATUS FROM MR_JOB_MAP_DATALOG  WHERE FILE_ID=? AND JOBID= ? GROUP BY STATUS";

	// 查询某个JOBID，某次失败的
	private static final String SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_PERTIMES = "SELECT ID, STATUS, FILE_ID, DATA_SOURCE_ID, FILE_PATH FROM MR_JOB_MAP_DATALOG WHERE JOBID=? AND JOB_LOG_ID= ? AND STATUS=3";
	private static final String SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_PERTIMES = "SELECT ID, STATUS, FILE_ID, DATA_SOURCE_ID, FILE_PATH FROM MR_JOB_MAP_DATALOG WHERE JOBID=? AND JOB_LOG_ID= ? AND STATUS=3";
	// 查询某个JOBID，某次段时间失败
	private static final String SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_PERTIME = "SELECT A.ID, A.STATUS, A.FILE_ID, A.JOB_LOG_ID,A. DATA_SOURCE_ID, A.FILE_PATH FROM MR_JOB_MAP_DATALOG A WHERE A.STATUS = 3 AND (A.DATA_SOURCE_ID, A.FILE_PATH, A.ID) IN (SELECT B.DATA_SOURCE_ID, B.FILE_PATH, MAX(B.id)  FROM MR_JOB_MAP_DATALOG B  WHERE B.JOBID = ? AND B.START_TIME BETWEEN ? AND ? GROUP BY B.DATA_SOURCE_ID, B.FILE_PATH)";
	private static final String SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_PERTIME = "SELECT A.ID, A.STATUS, A.FILE_ID, A.JOB_LOG_ID,A. DATA_SOURCE_ID, A.FILE_PATH FROM MR_JOB_MAP_DATALOG A WHERE A.STATUS = 3 AND (A.DATA_SOURCE_ID, A.FILE_PATH, A.ID) IN (SELECT B.DATA_SOURCE_ID, B.FILE_PATH, MAX(B.id)  FROM MR_JOB_MAP_DATALOG B  WHERE B.JOBID = ? AND B.START_TIME BETWEEN ? AND ? GROUP BY B.DATA_SOURCE_ID, B.FILE_PATH)";
	// 查询某个JOBID，所有失败的
	private static final String SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_ALLFAIL = "SELECT A.ID, A.STATUS, A.FILE_ID, A.JOB_LOG_ID,A. DATA_SOURCE_ID, A.FILE_PATH FROM MR_JOB_MAP_DATALOG A WHERE A.STATUS = 3 AND (A.DATA_SOURCE_ID, A.FILE_PATH, A.ID) IN (SELECT B.DATA_SOURCE_ID, B.FILE_PATH, MAX(B.id)  FROM MR_JOB_MAP_DATALOG B  WHERE B.JOBID = ? GROUP BY B.DATA_SOURCE_ID, B.FILE_PATH)";
	private static final String SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_ALLFAIL = "SELECT A.ID, A.STATUS, A.FILE_ID, A.JOB_LOG_ID,A. DATA_SOURCE_ID, A.FILE_PATH FROM MR_JOB_MAP_DATALOG A WHERE A.STATUS = 3 AND (A.DATA_SOURCE_ID, A.FILE_PATH, A.ID) IN (SELECT B.DATA_SOURCE_ID, B.FILE_PATH, MAX(B.id)  FROM MR_JOB_MAP_DATALOG B  WHERE B.JOBID = ? GROUP BY B.DATA_SOURCE_ID, B.FILE_PATH)";

	// 查询某个文件ID是否处理成功
	private static final String SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_SINGLE_FILE = "SELECT STATUS FROM MR_JOB_MAP_DATALOG WHERE ID=? AND JOB_LOG_ID= ?";
	private static final String SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_SINGLE_FILE = "SELECT STATUS FROM MR_JOB_MAP_DATALOG WHERE ID=? AND JOB_LOG_ID= ?";

	// 记录文件信息列表
	private static final String SQL_MR_FILE_LIST_MYSQL = "INSERT INTO MR_FILE_LIST(FILE_ID,DATA_SOURCE_ID,FILE_PATH,FILE_SIZE,RECORD_TIME,RECORD_MONTH) VALUES(?,?,?,?,?,?)";
	private static final String SQL_MR_FILE_LIST_ORCALE = "INSERT INTO MR_FILE_LIST(FILE_ID,DATA_SOURCE_ID,FILE_PATH,FILE_SIZE,RECORD_TIME,RECORD_MONTH) VALUES(?,?,?,?,?,?)";
	private static final String SQL_MR_FILE_LIST_QUERY = "SELECT FILE_ID,DATA_SOURCE_ID,FILE_PATH,FILE_SIZE,RECORD_TIME,RECORD_MONTH FROM MR_FILE_LIST WHERE FILE_ID = ?";

	private static final String QUERY_MR_DATA_SOURCE_PARAM = "SELECT C.PARAM_NAME, C.PARAM_VALUE, A.SOURCE_DB_TYPE FROM MR_SOURCE_TYPE A, MR_DATA_SOURCE B, MR_DATA_SOURCE_PARAM C WHERE A.SOURCE_TYPE_ID = B.SOURCE_TYPE_ID AND B.DATA_SOURCE_ID = C.DATA_SOURCE_ID AND B.DATA_SOURCE_ID = ?";
	private String poolKey;
	private static final int DB_SIGN_MYSQL = 1;
	private static final int DB_SIGN_ORCALE = 2;
	private int db_sign;

	public DBLogOutput(String url, String user, String password) {
		if (url != null && url.startsWith("jdbc:mysql")) {
			this.db_sign = DB_SIGN_MYSQL;
		} else if (url != null && url.startsWith("jdbc:oracle")) {
			this.db_sign = DB_SIGN_ORCALE;
		}
		try {
			this.poolKey = user + "/" + password + "@" + url;
			if (!DataSourceManager.containKey(poolKey)) {
				DataSourceImpl dsi = new DataSourceImpl(url, user, password);
				DataSourceManager.addDataSource(this.poolKey, dsi);//
			}
		} catch (SQLException e) {
			LogUtils.error("数据日志输出，数据源连接异常", e);
		}
	}

	@Override
	public synchronized void output(MRJobRunLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_INSERT_JOB_RUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_INSERT_JOB_RUNLOG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, log.getLogId());
			statment.setLong(2, log.getJobId());
			statment.setString(3, log.getMonthNo());
			statment.setString(4, log.getDataNo());
			statment.setString(5, StringUtil.dateToString(log.getStartDate(), StringUtil.DATE_FORMAT_TYPE1));
			statment.setLong(6, log.getAllFileSize());
			statment.setString(7, log.getExecCMD());
			statment.setString(8, log.getQueueName());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputUpdate(MRJobRunLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_UPDATE_JOB_RUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_UPDATE_JOB_RUNLOG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, StringUtil.dateToString(log.getEndDate(), StringUtil.DATE_FORMAT_TYPE1));
			statment.setLong(2, log.getRunFlag());
			statment.setLong(3, log.getRowRecord());
			statment.setString(4, log.getLogMSG());
			statment.setLong(5, log.getLogId());
			statment.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputMapCountUpdate(long logId, long inputCount, long outputCount, long recordInvalidCount) {
		Connection conn = null;
		PreparedStatement statment = null;
		PreparedStatement selectStatment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_UPDATE_JOB_RUNLOG_MAPCOUNT_MYSQL;
			break;
		case 2:
			sql = SQL_UPDATE_JOB_RUNLOG_MAPCOUNT_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			selectStatment = conn.prepareStatement(SQL_SELECT_JOB_RUNLOG_MAPCOUNT);
			selectStatment.setLong(1, logId);
			ResultSet rs = selectStatment.executeQuery();
			long mapInputCount = 0;
			long mapOuputCount = 0;
			long inputFilterCount = 0;
			while (rs.next()) {
				mapInputCount = rs.getLong(1);
				mapOuputCount = rs.getLong(2);
				inputFilterCount = rs.getLong(3);
			}

			statment = conn.prepareStatement(sql);
			statment.setLong(1, mapInputCount + inputCount);
			statment.setLong(2, mapOuputCount + outputCount);
			statment.setLong(3, inputFilterCount + recordInvalidCount);
			statment.setLong(4, logId);
			statment.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputReduceCountUpdate(long logId, long inputCount, long outputCount,
			long recordInvalidCount) {
		Connection conn = null;
		PreparedStatement statment = null;
		PreparedStatement selectStatment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_UPDATE_JOB_RUNLOG_REDUCECOUNT_MYSQL;
			break;
		case 2:
			sql = SQL_UPDATE_JOB_RUNLOG_REDUCECOUNT_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			selectStatment = conn.prepareStatement(SQL_SELECT_JOB_RUNLOG_REDUCECOUNT);
			selectStatment.setLong(1, logId);
			ResultSet rs = selectStatment.executeQuery();
			long reduceInputCount = 0;
			long reduceOuputCount = 0;
			long filterCount = 0;
			while (rs.next()) {
				reduceInputCount = rs.getLong(1);
				reduceOuputCount = rs.getLong(2);
				filterCount = rs.getLong(3);
			}

			statment = conn.prepareStatement(sql);
			statment.setLong(1, reduceInputCount + inputCount);
			statment.setLong(2, reduceOuputCount + outputCount);
			statment.setLong(3, filterCount + recordInvalidCount);
			statment.setLong(4, logId);
			statment.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public void outputFileSizeUpdate(long logId, long totalSize) {
		Connection conn = null;
		PreparedStatement statment = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(SQL_UPDATE_JOB_RUNLOG_FILESIZE);
			statment.setLong(1, totalSize);
			statment.setLong(2, logId);
			statment.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void output(MRJobMapRunLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_INSERT_JOB_MAPRUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_INSERT_JOB_MAPRUNLOG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, log.getMapTaskId());
			statment.setString(2, String.valueOf(log.getLogId()));
			statment.setString(3, StringUtil.dateToString(log.getStartDate(), StringUtil.DATE_FORMAT_TYPE1));
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public int queryJOBMapRunLogByMapTaskId(String taskId) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_QRY_JOB_MAPRUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_QRY_JOB_MAPRUNLOG_ORACLE;
			break;
		default:
			return 0;
		}

		ResultSet result = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, taskId);
			result = statment.executeQuery();
			while (result.next()) {
				return result.getInt("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return 0;
	}

	public int queryJOBMapRunLogByReduceTaskId(String taskId) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_QRY_JOB_REDUCERUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_QRY_JOB_REDUCERUNLOG_ORACLE;
			break;
		default:
			return 0;
		}

		ResultSet result = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, taskId);
			result = statment.executeQuery();
			while (result.next()) {
				return result.getInt("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return 0;
	}

	@Override
	public synchronized void outputUpdate(MRJobMapRunLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_UPDATE_JOB_MAPRUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_UPDATE_JOB_MAPRUNLOG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, log.getMapInputCount());
			statment.setLong(2, log.getMapOutputCount());
			statment.setString(3, StringUtil.dateToString(log.getEndDate(), StringUtil.DATE_FORMAT_TYPE1));
			statment.setInt(4, log.getRunFlag());
			statment.setString(5, log.getLogMSG());
			statment.setLong(6, log.getRecordInvalidCount());
			statment.setString(7, log.getMapTaskId());
			statment.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void output(MRJobReduceRunLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_INSERT_JOB_REDUCERUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_INSERT_JOB_REDUCERUNLOG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, log.getReduceTaskId());
			statment.setString(2, String.valueOf(log.getLogId()));
			statment.setString(3, StringUtil.dateToString(log.getStartDate(), StringUtil.DATE_FORMAT_TYPE1));
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputUpdate(MRJobReduceRunLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_UPDATE_JOB_REDUCERUNLOG_MYSQL;
			break;
		case 2:
			sql = SQL_UPDATE_JOB_REDUCERUNLOG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, log.getReduceInputCount());
			statment.setLong(2, log.getReduceOutputCount());
			statment.setString(3, StringUtil.dateToString(log.getEndDate(), StringUtil.DATE_FORMAT_TYPE1));
			statment.setInt(4, log.getRunFlag());
			statment.setString(5, log.getLogMSG());
			statment.setLong(6, log.getRecordInvalidCount());
			statment.setString(7, log.getReduceTaskId());
			statment.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputMapLogMsg(MRJobLogMsg log) {
		Connection conn = null;
		PreparedStatement statment = null;
		PreparedStatement sequneceStatment = null;

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			switch (this.db_sign) {
			case 1:
				statment = conn.prepareStatement(SQL_INSERT_JOB_MAPRUNLOG_MSG_MYSQL);
				statment.setString(1, log.getTaskId());
				statment.setInt(2, log.getLogType());
				statment.setString(3, StringUtil.dateToString(log.getLogDate(), StringUtil.DATE_FORMAT_TYPE1));
				statment.setString(4, log.getLogMsg());
				statment.execute();
				break;
			case 2:
				sequneceStatment = conn.prepareStatement(SEQUENCE_INSERT_JOB_MAPRUNLOG_MSG_ORCALE);
				ResultSet rs = sequneceStatment.executeQuery();
				long logId = 0;
				while (rs.next()) {
					logId = rs.getLong(1);
				}

				statment = conn.prepareStatement(SQL_INSERT_JOB_MAPRUNLOG_MSG_ORCALE);
				statment.setLong(1, logId);
				statment.setString(2, log.getTaskId());
				statment.setInt(3, log.getLogType());
				statment.setString(4, StringUtil.dateToString(log.getLogDate(), StringUtil.DATE_FORMAT_TYPE1));
				statment.setString(5, log.getLogMsg());
				statment.execute();
				break;
			default:
				return;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputReudceLogMsg(MRJobLogMsg log) {
		Connection conn = null;
		PreparedStatement statment = null;
		PreparedStatement sequneceStatment = null;

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			switch (this.db_sign) {
			case 1:
				statment = conn.prepareStatement(SQL_INSERT_JOB_REDUCERUNLOG_MSG_MYSQL);
				statment.setString(1, log.getTaskId());
				statment.setInt(2, log.getLogType());
				statment.setString(3, StringUtil.dateToString(log.getLogDate(), StringUtil.DATE_FORMAT_TYPE1));
				statment.setString(4, log.getLogMsg());
				statment.execute();
				break;
			case 2:
				sequneceStatment = conn.prepareStatement(SEQUENCE_INSERT_JOB_REDUCERUNLOG_MSG_ORCALE);
				ResultSet rs = sequneceStatment.executeQuery();
				long logId = 0;
				while (rs.next()) {
					logId = rs.getLong(1);
				}

				statment = conn.prepareStatement(SQL_INSERT_JOB_REDUCERUNLOG_MSG_ORCALE);
				statment.setLong(1, logId);
				statment.setString(2, log.getTaskId());
				statment.setInt(3, log.getLogType());
				statment.setString(4, StringUtil.dateToString(log.getLogDate(), StringUtil.DATE_FORMAT_TYPE1));
				statment.setString(5, log.getLogMsg());
				statment.execute();
			default:
				return;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputLogMsg(MRJobRunLogMsg log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_INSERT_JOB_RUNLOG_MSG_MYSQL;
			break;
		case 2:
			sql = SQL_INSERT_JOB_RUNLOG_MSG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, StringUtil.dateToString(log.getLogDate(), StringUtil.DATE_FORMAT_TYPE1));
			statment.setLong(2, log.getLogId());
			statment.setInt(3, log.getLogType());
			statment.setString(4, log.getLogMSG());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized void outputRemoteFileMsg(FTPLogPO ftpLogPO) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_INSERT_JOB_RUN_REMOTE_FILE_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_INSERT_JOB_RUN_REMOTE_FILE_LOG_ORACLE;
			break;
		default:
			return;
		}

		if (null == ftpLogPO || null == ftpLogPO.getInputFileMsg() || null == ftpLogPO.getOutputFileMsg()) {
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, ftpLogPO.getColId());
			statment.setLong(2, ftpLogPO.getColLogId());
			statment.setString(3, ftpLogPO.getInputFileLastModifyTime());
			statment.setInt(4, ftpLogPO.getStatus());
			statment.setString(5, ftpLogPO.getInputFileMsg());
			statment.setString(6, ftpLogPO.getOutputFileMsg());
			statment.setString(7, ftpLogPO.getRunDateTime());
			statment.setLong(8, ftpLogPO.getColFileDetailId());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized int[] outputRemoteFileMsgBatch(final List<FTPLogPO> lstlog) {
		Connection conn = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_INSERT_JOB_RUN_REMOTE_FILE_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_INSERT_JOB_RUN_REMOTE_FILE_LOG_ORACLE;
			break;
		default:
			return new int[0];
		}
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			DataAccess dataAccess = DataAccessFactory.getInstance(conn);
			return dataAccess.execUpdateBatch(sql, lstlog.size(), new IParamsSetter() {
				public void setValues(PreparedStatement statment, int i) throws SQLException {
					FTPLogPO ftpLogPO = lstlog.get(i);
					statment.setLong(1, ftpLogPO.getColId());
					statment.setLong(2, ftpLogPO.getColLogId());
					statment.setString(3, ftpLogPO.getInputFileLastModifyTime());
					statment.setInt(4, ftpLogPO.getStatus());
					statment.setString(5, ftpLogPO.getInputFileMsg());
					statment.setString(6, ftpLogPO.getOutputFileMsg());
					statment.setString(7, ftpLogPO.getRunDateTime());
					statment.setLong(8, ftpLogPO.getColFileDetailId());
				}

				public int batchSize() {
					return lstlog.size();
				}
			});
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
		return new int[0];
	}

	@Override
	public synchronized void outputUpdateRemoteFileMsg(FTPLogPO ftpLogPO) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_UPDATE_JOB_RUN_REMOTE_FILE_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_UPDATE_JOB_RUN_REMOTE_FILE_LOG_ORACLE;
			break;
		default:
			return;
		}

		if (null == ftpLogPO || null == ftpLogPO.getInputFileMsg() || null == ftpLogPO.getOutputFileMsg()) {
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, ftpLogPO.getInputFileLastModifyTime());
			statment.setInt(2, ftpLogPO.getStatus());
			statment.setString(3, ftpLogPO.getRunDateTime());
			statment.setLong(4, ftpLogPO.getColFileDetailId());
			statment.setLong(5, ftpLogPO.getRecordInvalidCount());
			statment.setLong(6, ftpLogPO.getColId());
			statment.setString(7, ftpLogPO.getInputFileMsg());
			statment.setString(8, ftpLogPO.getOutputFileMsg());
			statment.setLong(9, ftpLogPO.getColLogId());
			statment.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized int[] outputUpdateRemoteFileMsgBatch(final List<FTPLogPO> lstlog) {
		Connection conn = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_UPDATE_JOB_RUN_REMOTE_FILE_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_UPDATE_JOB_RUN_REMOTE_FILE_LOG_ORACLE;
			break;
		default:
			return new int[0];
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			DataAccess dataAccess = DataAccessFactory.getInstance(conn);
			return dataAccess.execUpdateBatch(sql, lstlog.size(), new IParamsSetter() {
				public void setValues(PreparedStatement statment, int i) throws SQLException {
					FTPLogPO ftpLogPO = lstlog.get(i);
					statment.setString(1, ftpLogPO.getInputFileLastModifyTime());
					statment.setInt(2, ftpLogPO.getStatus());
					statment.setString(3, ftpLogPO.getRunDateTime());
					statment.setLong(4, ftpLogPO.getColFileDetailId());
					statment.setLong(5, ftpLogPO.getRecordInvalidCount());
					statment.setLong(6, ftpLogPO.getColId());
					statment.setString(7, ftpLogPO.getInputFileMsg());
					statment.setString(8, ftpLogPO.getOutputFileMsg());
					statment.setLong(9, ftpLogPO.getColLogId());
				}

				public int batchSize() {
					return lstlog.size();
				}
			});
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
		return new int[0];
	}

	@Override
	public synchronized FTPLogPO getJobRemoteFileCreateDate(long colId, long colLogId, String inputFileinfo,
			String outputFileInfo) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_SELECT_JOB_RUN_REMOTE_FILE_LOG;
			break;
		case 2:
			sql = SQL_SELECT_JOB_RUN_REMOTE_FILE_LOG;
			break;
		default:
			return null;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, colId);
			statment.setString(2, inputFileinfo);
			statment.setString(3, outputFileInfo);
			statment.setLong(4, colLogId);
			ResultSet result = statment.executeQuery();
			while (result.next()) {
				FTPLogPO ftpPO = new FTPLogPO();
				ftpPO.setColId(colId);
				ftpPO.setColLogId(colLogId);
				ftpPO.setInputFileMsg(inputFileinfo);
				ftpPO.setOutputFileMsg(outputFileInfo);
				ftpPO.setStatus(result.getInt("STATUS"));
				ftpPO.setInputFileLastModifyTime(result.getString("INPUT_FILE_LASTMODIFY_DATE"));
				ftpPO.setColFileDetailId(result.getLong("COL_DETAIL_LOG_ID"));
				return ftpPO;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
		return null;
	}

	@Override
	public synchronized boolean isExistJobRemoteFile(long jobId, String inputFileinfo, String outputFileInfo) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_SELECT_JOB_RUN_REMOTE_FILE_LOG;
			break;
		case 2:
			sql = SQL_SELECT_JOB_RUN_REMOTE_FILE_LOG;
			break;
		default:
			return false;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, jobId);
			statment.setString(2, inputFileinfo);
			statment.setString(3, outputFileInfo);
			ResultSet result = statment.executeQuery();
			while (result.next()) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
		return false;
	}

	public void colFileLog(MRColFileLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_LOG_ORACLE;
			break;
		default:
			return;
		}
		long id = this.queryForNextVal(SEQUENCE_FTP_FILE_LOG_ORCALE);
		log.setColLogid(id);
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, id);
			statment.setLong(2, log.getColId());
			statment.setString(3, log.getStartTime());
			statment.setInt(4, log.getFileNum());
			statment.setLong(5, log.getFileTotalsize());
			statment.setLong(6, log.getStatus());
			statment.setString(7, log.getQueueName());
			statment.setString(8, log.getJobCmd());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public long getColFileLogStatus(MRColFileLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_LOG_STATUS_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_LOG_STATUS_ORACLE;
			break;
		default:
			return -1;
		}
		ResultSet result = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, log.getColLogid());
			statment.setLong(2, log.getColLogid());
			result = statment.executeQuery();
			while (result.next()) {
				return result.getLong("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return -1;
	}

	public long[] queryColTaskLogResult(long colLogId) {
		long count[] = new long[2];
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_LOG_RESULT_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_LOG_RESULT_ORACLE;
			break;
		default:
			return count;
		}

		ResultSet result = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, colLogId);
			statment.setLong(2, colLogId);
			result = statment.executeQuery();
			while (result.next()) {
				count[0] = result.getLong("SUCC_COUNT");
				count[1] = result.getLong("FAIL_COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return count;
	}

	public void updateColFileLog(MRColFileLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_LOG_UPDATE_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_LOG_UPDATE_ORACLE;
			break;
		default:
			return;
		}
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(SQL_FTP_FILE_LOG_SELECT_FILENUM);
			statment.setLong(1, log.getColLogid());
			ResultSet rs = statment.executeQuery();
			long filNum = 0;
			long filNumSize = 0;
			while (rs.next()) {
				filNum = rs.getLong("FILE_COUNT");
				filNumSize = rs.getLong("TOTAL_FILE_SIZE");
			}

			statment = conn.prepareStatement(sql);
			statment.setString(1, log.getEndTime());
			statment.setLong(2, log.getStatus());
			statment.setLong(3, filNum);
			statment.setLong(4, filNumSize);
			statment.setLong(5, log.getColLogid());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public void updateReRunColFileLog(MRColFileLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_LOG_UPDATE_RERUN_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_LOG_UPDATE_RERUN_ORACLE;
			break;
		default:
			return;
		}
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, log.getStartTime());
			statment.setLong(2, log.getStatus());
			statment.setLong(3, log.getColLogid());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public long colFileDetailLog(MRColDetailLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_DETAIL_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_DETAIL_LOG_ORACLE;
			break;
		default:
			return -1;
		}

		long id = this.queryForNextVal(SEQUENCE_FTP_FILE_DETAIL_LOG_ORCALE);
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, id);
			statment.setLong(2, log.getColLogId());
			statment.setLong(3, log.getColId());
			statment.setString(4, log.getStartTime());
			statment.setString(5, log.getInputFileName());
			statment.setString(6, log.getInputPath());
			statment.setString(7, log.getOutputPath());
			statment.setLong(8, log.getFileSize());
			statment.setInt(9, log.getStatus());
			statment.setInt(10, log.getIsOutputRename());
			statment.setInt(11, log.getIsMoveOutput());
			statment.setString(12, log.getMoveOutputPath());
			statment.setInt(13, log.getIsDoinputfiletype());
			statment.setString(14, log.getMoveInputPath());
			statment.setString(15, log.getInputRename());
			statment.setString(16, log.getOutputRename());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return id;
	}

	public long getColFileDetailLog() {
		return this.queryForNextVal(SEQUENCE_FTP_FILE_DETAIL_LOG_ORCALE);
	}

	/**
	 * 测试批量更新
	 * 
	 * @return
	 */
	public int[] addColFileDetailLogBatch(final List<MRColDetailLog> lstlog) {
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_DETAIL_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_DETAIL_LOG_ORACLE;
			break;
		default:
			return new int[0];
		}
		Connection conn = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			DataAccess dataAccess = DataAccessFactory.getInstance(conn);
			return dataAccess.execUpdateBatch(sql, lstlog.size(), new IParamsSetter() {
				public void setValues(PreparedStatement statment, int i) throws SQLException {
					MRColDetailLog log = lstlog.get(i);
					statment.setLong(1, log.getId());
					statment.setLong(2, log.getColLogId());
					statment.setLong(3, log.getColId());
					statment.setString(4, log.getStartTime());
					statment.setString(5, log.getInputFileName());
					statment.setString(6, log.getInputPath());
					statment.setString(7, log.getOutputPath());
					statment.setLong(8, log.getFileSize());
					statment.setInt(9, log.getStatus());
					statment.setInt(10, log.getIsOutputRename());
					statment.setInt(11, log.getIsMoveOutput());
					statment.setString(12, log.getMoveOutputPath());
					statment.setInt(13, log.getIsDoinputfiletype());
					statment.setString(14, log.getMoveInputPath());
					statment.setString(15, log.getInputRename());
					statment.setString(16, log.getOutputRename());
				}

				public int batchSize() {
					return lstlog.size();
				}
			});
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return null;
	}

	public void updateColFileDetailLog(MRColDetailLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_DETAIL_LOG_UPDATE_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_DETAIL_LOG_UPDATE_ORACLE;
			break;
		default:
			return;
		}
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, log.getEndTime());
			statment.setInt(2, log.getDeleteInputStatus());
			statment.setInt(3, log.getMoveInputStatus());
			statment.setInt(4, log.getMoveOutputStatus());
			statment.setString(5, log.getOutputFileName());
			statment.setInt(6, log.getOutputRenameStatus());
			statment.setInt(7, log.getStatus());
			statment.setLong(8, log.getFileId());
			statment.setLong(9, log.getRenameInputStatus());
			statment.setLong(10, log.getId());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public void colFileErrorLog(MRColFileErrorLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_FTP_FILE_ERROR_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_FTP_FILE_ERROR_LOG_ORACLE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, this.queryForNextVal(SEQUENCE_FTP_FILE_ERROR_LOG_ORCALE));
			statment.setLong(2, log.getColLogId());
			statment.setString(3, log.getCreatTime());
			statment.setString(4, log.getProtocol());
			statment.setString(5, log.getIp());
			statment.setInt(6, log.getPort());
			statment.setString(7, log.getUsername());
			statment.setString(8, log.getRootpath());
			statment.setInt(9, log.getType());
			statment.setString(10, log.getMsg());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public List<Object[]> getColFailList(long colId, long colLogId) {
		List<Object[]> lst = new ArrayList<Object[]>();
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = (colLogId <= 0) ? SQL_FTP_COL_ID_FAIL_LIST_MYSQL : SQL_FTP_COL_LOG_ID_FAIL_LIST_MYSQL;
			break;
		case 2:
			sql = (colLogId <= 0) ? SQL_FTP_COL_ID_FAIL_LIST_ORACLE : SQL_FTP_COL_LOG_ID_FAIL_LIST_ORACLE;
			break;
		default:
			return lst;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, colId);
			if (colLogId <= 0) {
				statment.setLong(2, colId);
				statment.setLong(3, colId);
			} else {
				statment.setLong(2, colId);
				statment.setLong(3, colLogId);
				statment.setLong(4, colId);
				statment.setLong(5, colLogId);
			}

			ResultSet result = statment.executeQuery();
			while (result.next()) {
				Object objArr[] = new Object[2];
				FTPLogPO ftpPO = new FTPLogPO();
				ftpPO.setInputFileLastModifyTime(result.getString("INPUT_FILE_LASTMODIFY_DATE"));
				ftpPO.setStatus(result.getInt("STATUS"));
				ftpPO.setInputFileMsg(result.getString("INPUT_FILE_MSG"));
				ftpPO.setOutputFileMsg(result.getString("OUTPUT_FILE_MSG"));
				ftpPO.setColFileDetailId(result.getLong("COL_DETAIL_LOG_ID"));

				MRColDetailLog detailLog = new MRColDetailLog();
				detailLog.setColId((int) colId);
				detailLog.setColLogId(result.getLong("COL_LOG_ID"));
				detailLog.setInputFileName(result.getString("INPUT_FILE_NAME"));
				detailLog.setOutputFileName(result.getString("OUTPUT_FILE_NAME"));
				detailLog.setInputPath(result.getString("INPUT_PATH"));
				detailLog.setOutputPath(result.getString("OUTPUT_PATH"));
				detailLog.setFileSize(result.getLong("FILE_SIZE"));
				detailLog.setStatus(result.getInt("DETAILS_STATUS"));
				detailLog.setIsOutputRename(result.getInt("IS_OUTPUT_RENAME"));
				detailLog.setOutputRenameStatus(result.getInt("OUTPUT_RENAME_STATUS"));
				detailLog.setIsMoveOutput(result.getInt("IS_MOVE_OUTPUT"));
				detailLog.setMoveOutputPath(result.getString("MOVE_OUTPUT_PATH"));
				detailLog.setMoveOutputStatus(result.getInt("MOVE_OUTPUT_STATUS"));
				detailLog.setIsDoinputfiletype(result.getInt("IS_DOINPUTFILETYPE"));
				detailLog.setDeleteInputStatus(result.getInt("DELETE_INPUT_STATUS"));
				detailLog.setMoveInputPath(result.getString("MOVE_INPUT_PATH"));
				detailLog.setMoveInputStatus(result.getInt("MOVE_INPUT_STATUS"));
				detailLog.setFileId(result.getLong("FILE_ID"));
				detailLog.setInputRename(result.getString("INPUT_RENAME"));
				detailLog.setRenameInputStatus(result.getInt("INPUT_RENAME_STATUS"));
				detailLog.setOutputRename(result.getString("OUTPUT_RENAME"));

				objArr[0] = ftpPO;
				objArr[1] = detailLog;
				lst.add(objArr);
			}
			return lst;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return lst;
	}

	public MRJobMapDataLog getMRJobMapDataLog(long jobid, String fileId) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY;
			break;
		default:
			return null;
		}

		MRJobMapDataLog dataLog = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, fileId);
			statment.setLong(2, jobid);
			ResultSet rs = statment.executeQuery();
			while (rs.next()) {
				dataLog = new MRJobMapDataLog();
				dataLog.setId(rs.getInt("ID"));
				dataLog.setJobid(String.valueOf(jobid));
				dataLog.setFileid(fileId);
				dataLog.setStatus(rs.getInt("STATUS"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return dataLog;
	}

	public List<MRJobMapDataLog> getMRJobMapDataLog(long jobid, long joblogid) {
		List<MRJobMapDataLog> dataLog = new ArrayList<MRJobMapDataLog>();
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_PERTIMES;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_PERTIMES;
			break;
		default:
			return dataLog;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, jobid);
			statment.setLong(2, joblogid);
			ResultSet rs = statment.executeQuery();
			while (rs.next()) {
				MRJobMapDataLog tml = new MRJobMapDataLog();
				tml.setId(rs.getInt("ID"));
				tml.setJobLogId(joblogid);
				tml.setJobid(String.valueOf(jobid));
				tml.setFileid(rs.getString("FILE_ID"));
				tml.setStatus(rs.getInt("STATUS"));
				tml.setDataSourceId(rs.getInt("DATA_SOURCE_ID"));
				tml.setFilePath(rs.getString("FILE_PATH"));
				tml.setHdfsAddress(this.getHDFSAddress(tml.getDataSourceId()));
				dataLog.add(tml);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return dataLog;
	}

	public List<MRJobMapDataLog> getMRJobMapDataLog(long jobid, String[] timeRange) {
		List<MRJobMapDataLog> dataLog = new ArrayList<MRJobMapDataLog>();
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_PERTIME;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_PERTIME;
			break;
		default:
			return dataLog;
		}

		if (timeRange.length != 2) {
			return dataLog;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, jobid);
			statment.setString(2, timeRange[0]);
			statment.setString(3, timeRange[1]);
			ResultSet rs = statment.executeQuery();
			while (rs.next()) {
				MRJobMapDataLog tml = new MRJobMapDataLog();
				tml.setId(rs.getInt("ID"));
				tml.setJobLogId(rs.getLong("JOB_LOG_ID"));
				tml.setJobid(String.valueOf(jobid));
				tml.setFileid(rs.getString("FILE_ID"));
				tml.setStatus(rs.getInt("STATUS"));
				tml.setDataSourceId(rs.getInt("DATA_SOURCE_ID"));
				tml.setFilePath(rs.getString("FILE_PATH"));
				tml.setHdfsAddress(this.getHDFSAddress(tml.getDataSourceId()));
				dataLog.add(tml);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return dataLog;
	}

	public List<MRJobMapDataLog> getMRJobMapDataLog(long jobid) {
		List<MRJobMapDataLog> dataLog = new ArrayList<MRJobMapDataLog>();
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_ALLFAIL;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_ALLFAIL;
			break;
		default:
			return dataLog;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, jobid);
			ResultSet rs = statment.executeQuery();
			while (rs.next()) {
				MRJobMapDataLog tml = new MRJobMapDataLog();
				tml.setId(rs.getInt("ID"));
				tml.setJobLogId(rs.getLong("JOB_LOG_ID"));
				tml.setJobid(String.valueOf(jobid));
				tml.setFileid(rs.getString("FILE_ID"));
				tml.setStatus(rs.getInt("STATUS"));
				tml.setDataSourceId(rs.getInt("DATA_SOURCE_ID"));
				tml.setFilePath(rs.getString("FILE_PATH"));
				tml.setHdfsAddress(this.getHDFSAddress(tml.getDataSourceId()));
				dataLog.add(tml);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return dataLog;
	}

	public int getMRJobMapDataLogSingleFile(long id, long joblogid) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL_QUERY_SINGLE_FILE;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE_QUERY_SINGLE_FILE;
			break;
		default:
			return -1;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, id);
			statment.setLong(2, joblogid);
			ResultSet rs = statment.executeQuery();
			while (rs.next()) {
				return rs.getInt("STATUS");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return -1;
	}

	public void updateMRJobMapDataLogStatus(long id, int status) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_UPDATE_MYSQL_STATUS;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_UPDATE_ORCALE_STATUS;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setInt(1, status);
			statment.setLong(2, id);
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public void addMRJobMapDataLog(MRJobMapDataLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE;
			break;
		default:
			return;
		}

		try {
			long id = this.queryForNextVal(SEQUENCE_MR_JOB_MAP_DATALOG_ORCALE);
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, id);
			statment.setString(2, log.getJobid());
			statment.setString(3, log.getFileid());
			statment.setLong(4, log.getDataType());
			statment.setString(5, log.getFilePath());
			statment.setLong(6, log.getFileSize());
			statment.setString(7, log.getStartTime());
			statment.setInt(8, log.getStatus());
			statment.setLong(9, log.getJobLogId());
			statment.setLong(10, log.getDataSourceId());
			statment.execute();
			log.setId(id);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public void addMRJobMapDataLogFTP(MRJobMapDataLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL_FTP;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE_FTP;
			break;
		default:
			return;
		}

		try {
			long id = this.queryForNextVal(SEQUENCE_MR_JOB_MAP_DATALOG_ORCALE);
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, id);
			statment.setString(2, log.getJobid());
			statment.setString(3, log.getFileid());
			statment.setString(4, log.getFilePath());
			statment.setLong(5, log.getFileSize());
			statment.setString(6, log.getStartTime());
			statment.setInt(7, log.getStatus());
			statment.setLong(8, log.getJobLogId());
			statment.setLong(9, log.getDataSourceId());
			statment.execute();
			log.setId(id);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public void setMRJobMapDataLogId(MRJobMapDataLog log) {
		log.setId(this.queryForNextVal(SEQUENCE_MR_JOB_MAP_DATALOG_ORCALE));
	}

	/**
	 * 测试批量更新
	 * 
	 * @return
	 */
	public int[] addMRJobMapDataLogFTPBatch(final List<MRJobMapDataLog> lstlog) {
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_MYSQL_FTP;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_ORCALE_FTP;
			break;
		default:
			return new int[0];
		}
		Connection conn = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			DataAccess dataAccess = DataAccessFactory.getInstance(conn);
			return dataAccess.execUpdateBatch(sql, lstlog.size(), new IParamsSetter() {
				public void setValues(PreparedStatement statment, int i) throws SQLException {
					MRJobMapDataLog log = lstlog.get(i);
					statment.setLong(1, log.getId());
					statment.setString(2, log.getJobid());
					statment.setString(3, log.getFileid());
					statment.setString(4, log.getFilePath());
					statment.setLong(5, log.getFileSize());
					statment.setString(6, log.getStartTime());
					statment.setInt(7, log.getStatus());
					statment.setLong(8, log.getJobLogId());
					statment.setLong(9, log.getDataSourceId());
				}

				public int batchSize() {
					return lstlog.size();
				}
			});
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return new int[0];
	}

	public void addFileListLog(MRFileListPO filePO) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_FILE_LIST_MYSQL;
			break;
		case 2:
			sql = SQL_MR_FILE_LIST_ORCALE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, filePO.getFileId());
			statment.setLong(2, filePO.getDataSourceId());
			statment.setString(3, filePO.getFilePath());
			statment.setLong(4, filePO.getFileSize());
			statment.setString(5, filePO.getRecordTime());
			statment.setString(6, filePO.getRecordMonth());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	@Override
	public synchronized MRFileListPO getFilePoByFileId(String fileId) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_FILE_LIST_QUERY;
			break;
		case 2:
			sql = SQL_MR_FILE_LIST_QUERY;
			break;
		default:
			return null;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, fileId);
			ResultSet result = statment.executeQuery();
			while (result.next()) {
				MRFileListPO mrfilePO = new MRFileListPO();
				mrfilePO.setDataSourceId(result.getInt("DATA_SOURCE_ID"));
				mrfilePO.setFileId(fileId);
				mrfilePO.setFilePath(result.getString("FILE_PATH"));
				mrfilePO.setFileSize(result.getLong("FILE_SIZE"));
				mrfilePO.setRecordMonth(result.getString("RECORD_MONTH"));
				mrfilePO.setRecordTime(result.getString("RECORD_TIME"));
				return mrfilePO;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
		return null;
	}

	public void updateMRJobMapDataLog(MRJobMapDataLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_UPDATE_MYSQL;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_UPDATE_ORCALE;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, log.getEndTime());
			statment.setLong(2, log.getTotalCount());
			statment.setLong(3, log.getSuccessCount());
			statment.setLong(4, log.getFailCount());
			statment.setInt(5, log.getStatus());
			statment.setString(6, log.getFilterPath());
			statment.setLong(7, log.getId());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	public void updateMRJobMapDataLogFTP(MRJobMapDataLog log) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_MR_JOB_MAP_DATALOG_UPDATE_MYSQL_FTP;
			break;
		case 2:
			sql = SQL_MR_JOB_MAP_DATALOG_UPDATE_ORCALE_FTP;
			break;
		default:
			return;
		}

		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setString(1, log.getEndTime());
			statment.setLong(2, log.getTotalCount());
			statment.setLong(3, log.getSuccessCount());
			statment.setLong(4, log.getFailCount());
			statment.setInt(5, log.getStatus());
			statment.setString(6, log.getFilterPath());
			statment.setLong(7, log.getDataType());
			statment.setLong(8, log.getId());
			statment.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
	}

	/**
	 * 得到详细的日志信息
	 * 
	 * @param detailLogId
	 * @return
	 */
	public MRColDetailLog getDetailLog(long detailLogId) {
		MRColDetailLog detailLog = null;
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_SELECT_DETAIL_FILE_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_SELECT_DETAIL_FILE_LOG_ORACLE;
			break;
		default:
			return null;
		}
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, detailLogId);
			ResultSet rs = statment.executeQuery();
			while (rs.next()) {
				detailLog = new MRColDetailLog();
				detailLog.setStatus(rs.getInt("STATUS"));
				detailLog.setId(rs.getInt("ID"));
				detailLog.setRenameInputStatus(rs.getInt("INPUT_RENAME_STATUS"));
				detailLog.setMoveInputStatus(rs.getInt("MOVE_INPUT_STATUS"));
				detailLog.setDeleteInputStatus(rs.getInt("DELETE_INPUT_STATUS"));
				detailLog.setIsDoinputfiletype(rs.getInt("IS_DOINPUTFILETYPE"));
				detailLog.setMoveOutputStatus(rs.getInt("MOVE_OUTPUT_STATUS"));
				detailLog.setOutputRenameStatus(rs.getInt("OUTPUT_RENAME_STATUS"));
				detailLog.setInputFileName(rs.getString("INPUT_FILE_NAME"));
				detailLog.setInputPath(rs.getString("INPUT_PATH"));
				detailLog.setInputRename(rs.getString("INPUT_RENAME"));
				detailLog.setMoveInputPath(rs.getString("MOVE_INPUT_PATH"));
				detailLog.setOutputFileName(rs.getString("OUTPUT_FILE_NAME"));
				detailLog.setOutputPath(rs.getString("OUTPUT_PATH"));
				detailLog.setOutputRename(rs.getString("OUTPUT_RENAME"));
				detailLog.setMoveOutputPath(rs.getString("MOVE_OUTPUT_PATH"));
				return detailLog;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
		return null;
	}

	/**
	 * 得到详细与输入日志信息数组
	 * 
	 * @param colId
	 * @param colLogId
	 * @param colFileDetailId
	 * @return
	 */

	public Object[] getMRColDetailLogNeedUpdate(long colId, long colLogId, String inputFileinfo, String outputFileInfo) {
		Object[] objArr = new Object[2];
		MRColDetailLog detailLog = null;
		FTPLogPO ftpPO = null;
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_SELECT_JOB_RUN_REMOTE_DETAIL_FILE_LOG_MYSQL;
			break;
		case 2:
			sql = SQL_SELECT_JOB_RUN_REMOTE_DETAIL_FILE_LOG_ORACLE;
			break;
		default:
			return null;
		}
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, colId);
			statment.setString(2, inputFileinfo);
			statment.setString(3, outputFileInfo);
			statment.setLong(4, colLogId);
			ResultSet result = statment.executeQuery();
			while (result.next()) {
				ftpPO = new FTPLogPO();
				ftpPO.setColId(colId);
				ftpPO.setColLogId(colLogId);
				ftpPO.setInputFileMsg(inputFileinfo);
				ftpPO.setOutputFileMsg(outputFileInfo);
				ftpPO.setStatus(result.getInt("STATUS"));
				ftpPO.setInputFileLastModifyTime(result.getString("INPUT_FILE_LASTMODIFY_DATE"));
				ftpPO.setColFileDetailId(result.getLong("COL_DETAIL_LOG_ID"));

				detailLog = new MRColDetailLog();
				detailLog.setColId((int) colId);
				detailLog.setStatus(result.getInt("DETAILS_STATUS"));
				detailLog.setIsDoinputfiletype(result.getInt("IS_DOINPUTFILETYPE"));
				detailLog.setMoveInputStatus(result.getInt("MOVE_INPUT_STATUS"));
				detailLog.setRenameInputStatus(result.getInt("INPUT_RENAME_STATUS"));
				detailLog.setDeleteInputStatus(result.getInt("DELETE_INPUT_STATUS"));
				detailLog.setIsMoveOutput(result.getInt("IS_MOVE_OUTPUT"));
				detailLog.setIsOutputRename(result.getInt("IS_OUTPUT_RENAME"));
				detailLog.setMoveOutputStatus(result.getInt("MOVE_OUTPUT_STATUS"));
				detailLog.setOutputRenameStatus(result.getInt("OUTPUT_RENAME_STATUS"));

				objArr[0] = ftpPO;
				objArr[1] = detailLog;
				return objArr;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}
		return null;
	}

	/**
	 * 查询采集的最新日志ID
	 * 
	 * @param colId
	 * @param colLogId
	 * @param colFileDetailId
	 * @return
	 */

	public long getJobMRColLastLogId(long colId) {
		Connection conn = null;
		PreparedStatement statment = null;
		String sql = "";
		switch (this.db_sign) {
		case 1:
			sql = SQL_SELECT_JOB_RUN_COL_LAST_LOGID_MYSQL;
			break;
		case 2:
			sql = SQL_SELECT_JOB_RUN_COL_LAST_LOGID_ORACLE;
			break;
		default:
			return -1;
		}
		long id = -1;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statment = conn.prepareStatement(sql);
			statment.setLong(1, colId);
			ResultSet result = statment.executeQuery();
			while (result.next()) {
				id = result.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		if (id <= 0) {// 日志ID必须大于0
			id = -1;
		}
		return id;
	}

	/**
	 * 查询序列的下一个值
	 * 
	 * @param scequenceName
	 *            序列名称
	 * @return
	 */
	public long queryForNextVal(String scequenceName) {
		String sql = "SELECT " + scequenceName + ".NEXTVAL FROM DUAL";
		Connection conn = null;
		PreparedStatement sequneceStatment = null;
		long logId = -1;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			sequneceStatment = conn.prepareStatement(sql);
			ResultSet rs = sequneceStatment.executeQuery();
			while (rs.next()) {
				logId = rs.getLong(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DataSourceManager.destroy();
		}

		return logId;
	}

	/**
	 * 设置ftp的数据源参数
	 * 
	 * @param ds
	 * @param jobId
	 * @throws Exception
	 */
	public String getHDFSAddress(int datasourceId) throws SQLException {
		ResultSet result = null;
		PreparedStatement statement = null;
		Connection conn = null;
		String protocol = null;
		String ip = null;
		String port = null;
		String hdfsAddress = null;
		try {
			conn = DBRequestJob.getConnection(this.poolKey);// this.pool.getConnection();
			statement = conn.prepareStatement(QUERY_MR_DATA_SOURCE_PARAM);
			statement.setInt(1, datasourceId);
			result = statement.executeQuery();
			while (result.next()) {
				String name = result.getString("PARAM_NAME");
				String value = result.getString("PARAM_VALUE");
				int type = result.getInt("SOURCE_DB_TYPE");
				switch (type) {
				case 1:
					if ("mr.mapred.fs.default.name".equalsIgnoreCase(name)) {
						hdfsAddress = value;
					}
					break;
				case 6:
					if ("mr.mapred.ftp.protocol".equalsIgnoreCase(name)) {
						protocol = value;
					} else if ("mr.mapred.ftp.ip".equalsIgnoreCase(name)) {
						ip = value;
					} else if ("mr.mapred.ftp.port".equalsIgnoreCase(name)) {
						port = value;
					}
					break;
				default:
					break;
				}
			}

			if (null != hdfsAddress) {
				return hdfsAddress;
			}

			if (Contant.PROTOCOL_REMOREHDFS.equals(protocol) || Contant.PROTOCOL_LOCALHDFS.equals(protocol)) {
				return HDFSUtils.getHDFSAddress(ip, port);
			}
		} catch (SQLException e) {
			throw e;
		} finally {
			DataSourceManager.destroy();
		}
		return null;
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
	 * 关闭statment
	 * 
	 * @param statment
	 */
	private void closeStatment(PreparedStatement statment) {
		try {
			if (null != statment) {
				statment.close();
			}
		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}

	/**
	 * 
	 * @param conn
	 */
	private void releaseConn(Connection conn) {
		try {
			if (null != conn) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}