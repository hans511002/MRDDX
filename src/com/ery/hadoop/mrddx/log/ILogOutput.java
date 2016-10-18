package com.ery.hadoop.mrddx.log;

import java.util.List;

import com.ery.hadoop.mrddx.mode.MRFileListPO;
import com.ery.hadoop.mrddx.remote.FTPLogPO;

/**
 * 输出日志接口
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-4
 * @version v1.0
 */
public interface ILogOutput {
	/**
	 * 输出数据到日志表(MR_JOB_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void output(MRJobRunLog log);

	/**
	 * 更新数据到日志表(MR_JOB_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputUpdate(MRJobRunLog log);

	/**
	 * 更新Map数据（记录数）到日志表(MR_JOB_REDUCE_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputMapCountUpdate(long logId, long inputCount, long outputCount, long recordInvalidCount);

	/**
	 * 更新Reduce数据（记录数）到日志表(MR_JOB_REDUCE_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputReduceCountUpdate(long logId, long inputCount, long outputCount, long recordInvalidCount);

	/**
	 * 输出数据到日志表(MR_JOB_MAP_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void output(MRJobMapRunLog log);

	/**
	 * 更新数据到日志表(MR_JOB_MAP_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputUpdate(MRJobMapRunLog log);

	/**
	 * 输出数据到日志表(MR_JOB_REDUCE_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void output(MRJobReduceRunLog log);

	/**
	 * 更新数据到日志表(MR_JOB_REDUCE_RUN_LOG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputUpdate(MRJobReduceRunLog log);

	/**
	 * 输出数据到日志表(MR_JOB_MAP_RUN_LOG_MSG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputMapLogMsg(MRJobLogMsg log);

	/**
	 * 输出数据到日志表(MR_JOB_REDUCE_RUN_LOG_MSG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputReudceLogMsg(MRJobLogMsg log);

	/**
	 * 输出数据到日志表(MR_JOB_RUN_LOG_MSG)中
	 * 
	 * @param log 日志对象
	 */
	public void outputLogMsg(MRJobRunLogMsg log);

	/**
	 * 输出数据到日志表(MR_FTP_COL_REMOTE_FILE_LOG_MSG)中 输出整个JOB与远程文件的日志信息
	 * 
	 * @param log 日志对象
	 */
	public void outputRemoteFileMsg(FTPLogPO ftpLogPO);

	/**
	 * 输出数据到日志表(MR_FTP_COL_REMOTE_FILE_LOG_MSG)中 输出整个JOB与远程文件的日志信息
	 * 
	 * @param jobId
	 * @param fileCreateDate
	 * @param fileinfo
	 */
	public void outputUpdateRemoteFileMsg(FTPLogPO ftpLogPO);

	/**
	 * 获取文件创建的时间，日志表(MR_FTP_COL_REMOTE_FILE_LOG_MSG)
	 * 
	 * @param jobId
	 * @param fileinfo
	 */
	public FTPLogPO getJobRemoteFileCreateDate(long jobId, long colLogId, String inputFileinfo, String outputFileInfo);

	/**
	 * 根据文件ID查询文件信息
	 * 
	 * @param fileId
	 * @return
	 */
	public MRFileListPO getFilePoByFileId(String fileId);

	/**
	 * 判断记录是否存在，日志表(MR_FTP_COL_REMOTE_FILE_LOG_MSG)
	 * 
	 * @param jobId
	 * @param fileinfo
	 */
	public boolean isExistJobRemoteFile(long jobId, String inputFileinfo, String outputFileInfo);

	/**
	 * 批量提交日志
	 */
	public int[] outputRemoteFileMsgBatch(final List<FTPLogPO> lstlog);

	/**
	 * 批量提交日志
	 */
	public int[] outputUpdateRemoteFileMsgBatch(List<FTPLogPO> lstlog);

	/**
	 * 查询任务日志是否存在
	 * 
	 * @param taskId
	 * @return
	 */
	public int queryJOBMapRunLogByMapTaskId(String taskId);
}
