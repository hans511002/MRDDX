package com.ery.hadoop.mrddx.remote;

import org.apache.hadoop.conf.Configuration;

import com.ery.hadoop.mrddx.MRConfiguration;

/**
 * 与FTP有关的配置信息
 * 



 * @createDate 2013-2-21
 * @version v1.0
 */
public class FTPConfiguration extends MRConfiguration {
	// to hdfs任务前缀
	public static final String DOWN_PREFIX = "downftp_";
	// hdfs to 任务前缀
	public static final String UP_PREFIX = "upftp_";
	
	public static final String MAPRED_DOWNFTP_FS_DEFAULT_NAME = "downftp.mapred.fs.default.name";
	public static final String MAPRED_UPFTP_FS_DEFAULT_NAME = "upftp.mapred.fs.default.name";
	public static final String MAPRED_DOWNFTP_JOB_TRACKER_NAME = "downftp.mapred.job.tracker.name";
	public static final String MAPRED_UPFTP_JOB_TRACKER_NAME = "upftp.mapred.job.tracker.name";
	
	public static final String COL_STATUS_ALLOW = "0";
	public static final String COL_STATUS_FORBID = "1";
	
	public static final int CONSTANT_COL_TYPE_DOWN = 0; // 下载标示符
	public static final int CONSTANT_COL_TYPE_UP = 1; // 上传标示符
	
	public static final String COL_CURRENT_RUN_FLAG = "input.mr.mapred.ftp.collect.current.run.flag";
	public static final int COL_CURRENT_RUN_FLAG_DOWN = 0;
	public static final int COL_CURRENT_RUN_FLAG_UP = 1;
	
	public static final String COL_LOG_ID = "input.mr.mapred.ftp.collect.log.id";// 采集的日志ID
	public static final String COL_REDCORD_ERROR_INPUT_MSG = "input.mr.mapred.ftp.redcord.error.input.msg";// 临时记录采集的错误输入信息
	public static final String COL_REDCORD_ERROR_OUTPUT_MSG = "input.mr.mapred.ftp.redcord.error.output.msg";// 临时记录采集的错误输出信息

	// 下载任务
	public static final String COL_ID = "input.mr.mapred.ftp.collect.id";
	public static final String COL_NAME = "input.mr.mapred.ftp.collect.name";
	public static final String COL_TYPE = "input.mr.mapred.ftp.collect.type";// 0:下载, 1:上传
	public static final String COL_DATATYPE = "input.mr.mapred.ftp.collect.datatype";// 0:文本文件,1:其他文件
	public static final String COL_STATUS = "input.mr.mapred.ftp.collect.status";//0:启用, 1:禁用
	public static final String COL_TASK_NUMBER = "input.mr.mapred.ftp.collect.task.number";// map task任务数
	public static final String COL_TASK_PRIORITY = "input.mr.mapred.ftp.collect.task.priority";// 优先级
	public static final String COL_SYS_INPUTPATH = "input.mr.mapred.ftp.collect.sys.inputpath";// 系统运行的输入目录
	public static final String COL_FORCE_REPEAT = "input.mr.mapred.ftp.collect.force.repeat"; // 0:强制，或者重复下载
	public static final String COL_RERUN_DOWN_LOGID = "input.mr.mapred.ftp.collect.force.rerun.logid"; // 重新执行时，需要的下载日志id
	public static final String COL_FAIL = "input.mr.mapred.ftp.collect.fail"; //0:执行失败的下载任务
	
	
	/**
	 * 文件(下载)
	 */
	// 数据来源传输参数
	public static final String FTP_DOWN_MR_MAPRED_PARAM = "input.mr.mapred.ftp.down.param";
	// 存放解析后的参数
	public static final String FTP_DOWN_MR_MAPRED_PARAM_NEW = "input.mr.mapred.ftp.down.param.new";
	
	/**
	 * 文件(上传)
	 */
	// 数据来源传输参数
	public static final String FTP_UP_MR_MAPRED_PARAM = "ouput.mr.mapred.ftp.up.param";
	// 存放解析后的参数
	public static final String FTP_UP_MR_MAPRED_PARAM_NEW = "ouput.mr.mapred.ftp.up.param.new";
	
	/**
	 * 默认构造方法
	 * 
	 * @param job
	 *            job对象
	 */
	public FTPConfiguration(Configuration job) {
		super(job);
		this.conf = job;
	}

	public String getUpFSDefault(){
		return conf.get(MAPRED_UPFTP_FS_DEFAULT_NAME, null);
	}
	
	public String getUpJobTracker(){
		return conf.get(MAPRED_UPFTP_JOB_TRACKER_NAME, null);
	}
	
	public int getFtpDownCollectId(){
		return conf.getInt(DOWN_PREFIX+COL_ID, -1);
	}
	
	public int getFtpDownCollectType(){
		return conf.getInt(DOWN_PREFIX+COL_TYPE, -1);
	}
	
	public int getFtpDownCollectDataType(){
		return conf.getInt(DOWN_PREFIX+COL_DATATYPE, -1);
	}
	
	public int getFtpDownCollectStatus(){
		return conf.getInt(DOWN_PREFIX+COL_STATUS, -1);
	}
	
	public int getFtpUpCollectId(){
		return conf.getInt(UP_PREFIX+COL_ID, -1);
	}
	
	public String getFtpUpCollectName(){
		return conf.get(UP_PREFIX+COL_NAME, "up collect");
	}
	
	public int getFtpUpCollectType(){
		return conf.getInt(UP_PREFIX+COL_TYPE, -1);
	}
	
	public int getFtpUpCollectDataType(){
		return conf.getInt(UP_PREFIX+COL_DATATYPE, -1);
	}
	
	public int getFtpUpCollectStatus(){
		return conf.getInt(UP_PREFIX+COL_STATUS, -1);
	}
	
	public boolean getFtpDownCollectForceRepeat(){
		int flag = conf.getInt(DOWN_PREFIX+COL_FORCE_REPEAT, -1);
		return 0 == flag?true:false;
	}
	
	public void setFtpDownCollectForceRepeat(int flag){
		conf.setInt(DOWN_PREFIX+COL_FORCE_REPEAT, flag);
	}
	
	public boolean getFtpDownFail(){
		int flag = conf.getInt(DOWN_PREFIX+COL_FAIL, -1);
		return 0 == flag?true:false;
	}
	
	public void setFtpDownFail(int flag){
		conf.setInt(DOWN_PREFIX+COL_FAIL, flag);
	}
	public boolean getFtpUpFail(){
		int flag = conf.getInt(UP_PREFIX+COL_FAIL, -1);
		return 0 == flag?true:false;
	}
	
	public void setFtpUpFail(int flag){
		conf.setInt(UP_PREFIX+COL_FAIL, flag);
	}
	
	public boolean getFtpUpCollectForceRepeat(){
		int flag = conf.getInt(UP_PREFIX+COL_FORCE_REPEAT, -1);
		return 0 == flag?true:false;
	}
	
	public void setFtpUpCollectForceRepeat(int flag){
		conf.setInt(UP_PREFIX+COL_FORCE_REPEAT, flag);
	}
	
	public long getFtpDownCollectRepeatLogId(){
		return conf.getLong(DOWN_PREFIX+COL_RERUN_DOWN_LOGID, -1);
	}
	
	public void setFtpDownCollectRepeatLogId(long flag){
		conf.setLong(DOWN_PREFIX+COL_RERUN_DOWN_LOGID, flag);
	}
	
	public long getFtpUpCollectRepeatLogId(){
		return conf.getLong(UP_PREFIX+COL_RERUN_DOWN_LOGID, -1);
	}
	
	public void setFtpUpCollectRepeatLogId(long flag){
		conf.setLong(UP_PREFIX+COL_RERUN_DOWN_LOGID, flag);
	}
	
	
	public String getFtpDownMappedParam() {
		return conf.get(FTP_DOWN_MR_MAPRED_PARAM, null);
	}

	public void setFtpDownMappedParam(String param) {
		conf.set(FTP_DOWN_MR_MAPRED_PARAM, param);
	}
	
	public String getFtpDownMappedParamNew() {
		return conf.get(FTP_DOWN_MR_MAPRED_PARAM_NEW, null);
	}

	public void setFtpDownMappedParamNew(String param) {
		conf.set(FTP_DOWN_MR_MAPRED_PARAM_NEW, param);
	}
	
	public String getFtpUpMappedParam() {
		return conf.get(FTP_UP_MR_MAPRED_PARAM, null);
	}

	public void setFtpUpMappedParam(String param) {
		conf.set(FTP_UP_MR_MAPRED_PARAM, param);
	}
	
	public String getFtpUpMappedParamNew() {
		return conf.get(FTP_UP_MR_MAPRED_PARAM_NEW, null);
	}

	public void setFtpUpMappedParamNew(String param) {
		conf.set(FTP_UP_MR_MAPRED_PARAM_NEW, param);
	}
	
	public void setCurrentRunFlag(int flag) {
		conf.setInt(COL_CURRENT_RUN_FLAG, flag);
	}
	
	public int getCurrentRunFlag() {
		return conf.getInt(COL_CURRENT_RUN_FLAG, -1);
	}
	
	public void setFtpRedcordErrorInputMsg(String redcordErrorInputMsg) {
		conf.set(COL_REDCORD_ERROR_INPUT_MSG, redcordErrorInputMsg);
	}

	public String getFtpRedcordErrorInputMsg() {
		return conf.get(COL_REDCORD_ERROR_INPUT_MSG, "");
	}

	public void setFtpRedcordErrorOuputMsg(String redcordErrorOutputMsg) {
		conf.set(COL_REDCORD_ERROR_OUTPUT_MSG, redcordErrorOutputMsg);
	}
	
	public String getFtpRedcordErrorOuputMsg() {
		return conf.get(COL_REDCORD_ERROR_OUTPUT_MSG, "");
	}
}
