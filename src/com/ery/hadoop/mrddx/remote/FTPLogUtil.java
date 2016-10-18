package com.ery.hadoop.mrddx.remote;

import java.io.File;

import org.apache.commons.IFileTransfer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ery.hadoop.mrddx.util.StringUtil;

public class FTPLogUtil {

	public static String getInputFileCreateDate(Object inputSourceSystem, String inputSourceProtocol, String inputPath) {
		String inputFileCreateDate = null;
		try {
			// 获取输入的文件日期
			if (Contant.PROTOCOL_LOCAL.equals(inputSourceProtocol)) {
				File file = new File(inputPath);
				inputFileCreateDate = StringUtil.longToString(file.lastModified(), StringUtil.DATE_FORMAT_TYPE1);
			} else if (Contant.PROTOCOL_LOCALHDFS.equals(inputSourceProtocol) && inputSourceSystem instanceof FileSystem) {
				Path path = new Path(inputPath);
				long time = ((FileSystem) inputSourceSystem).getFileStatus(path).getModificationTime();
				inputFileCreateDate = StringUtil.longToString(time, StringUtil.DATE_FORMAT_TYPE1);
			} else if (Contant.PROTOCOL_REMOREHDFS.equals(inputSourceProtocol) && inputSourceSystem instanceof FileSystem) {
				Path path = new Path(inputPath);
				long time = ((FileSystem) inputSourceSystem).getFileStatus(path).getModificationTime();
				inputFileCreateDate = StringUtil.longToString(time, StringUtil.DATE_FORMAT_TYPE1);
			} else if (Contant.PROTOCOL_FTP.equals(inputSourceProtocol) && inputSourceSystem instanceof IFileTransfer) {
				long time = ((IFileTransfer) inputSourceSystem).getModificationTime(inputPath);
				inputFileCreateDate = StringUtil.longToString(time, StringUtil.DATE_FORMAT_TYPE1);
			} else if (Contant.PROTOCOL_SFTP.equals(inputSourceProtocol) && inputSourceSystem instanceof IFileTransfer) {
				long time = ((IFileTransfer) inputSourceSystem).getModificationTime(inputPath);
				inputFileCreateDate = StringUtil.longToString(time, StringUtil.DATE_FORMAT_TYPE1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return inputFileCreateDate;
	}
}
