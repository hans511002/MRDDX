package com.ery.hadoop.mrddx.remote;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.FileAttribute;
import org.apache.commons.IFileTransfer;
import org.apache.commons.ftp.edtftpj.FTPTransfer;
import org.apache.commons.sftp.jsch.SFTPTransfer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import com.enterprisedt.net.ftp.FTPException;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.log.MRColDetailLog;
import com.ery.hadoop.mrddx.log.MRColFileErrorLog;
import com.ery.hadoop.mrddx.log.MRColFileLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.GZIPUtils;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.JDBCUtil;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.base.support.utils.Convert;

public class SourceUtil {
	public static void rename(File file, String suffi) {
		if (file.isFile()) {
			file.renameTo(new File(file.getParent() + "/" + file.getName() + suffi));
		}
		for (File f : file.listFiles()) {
			if (f.isFile()) {
				f.renameTo(new File(f.getParent() + "/" + f.getName() + suffi));
			} else {
				rename(f, suffi);
			}
		}
	}

	// public static Object deserializable(String encode) throws IOException,
	// ClassNotFoundException {
	// if (null == encode) {
	// return null;
	// }
	// BASE64Decoder base64Decoder = new BASE64Decoder();
	// byte buf[] = base64Decoder.decodeBuffer(encode);
	// ByteArrayInputStream byteArrayInputStream = new
	// ByteArrayInputStream(buf);
	// ObjectInput input = new ObjectInputStream(byteArrayInputStream);
	// return input.readObject();
	// }

	// public static String serializable(Object source) throws IOException {
	// if (null == source) {
	// return null;
	// }
	// ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
	// ObjectOutput output = new ObjectOutputStream(byteStream);
	// output.writeObject(source);
	// BASE64Encoder base64Encoder = new BASE64Encoder();
	// return base64Encoder.encode(byteStream.toByteArray());
	// }

	/**
	 * 将对象序列化成字符串
	 * 
	 * @param obj
	 * @return
	 * @throws IOException
	 */
	public static String serializable(Object obj) throws IOException {
		if (null == obj) {
			return null;
		}
		return serializable(obj, true, true);
	}

	public static String serializable(Object obj, boolean isGzip, boolean urlEnCode) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		String serStr = null;
		byte[] bts = null;
		if (isGzip) {
			bts = GZIPUtils.zip(byteArrayOutputStream.toByteArray());
		} else {
			bts = byteArrayOutputStream.toByteArray();
		}
		if (urlEnCode) {
			serStr = new String(org.apache.commons.codec.binary.Base64.encodeBase64(bts), "ISO-8859-1");
		} else {
			serStr = new String(bts, "ISO-8859-1");
		}
		objectOutputStream.close();
		byteArrayOutputStream.close();
		return serStr;
	}

	/**
	 * 反序列化对象
	 * 
	 * @param serStr
	 * @return
	 * @throws IOException
	 */
	public static Object deserializable(String serStr) throws IOException {
		if (null == serStr) {
			return null;
		}
		return deserializable(serStr, true, true);
	}

	public static Object deserializable(String serStr, boolean isGzip, boolean urlEnCode) throws IOException {
		byte[] bts = null;
		if (urlEnCode) {
			bts = org.apache.commons.codec.binary.Base64.decodeBase64(serStr.getBytes("ISO-8859-1"));
		} else {
			bts = serStr.getBytes("ISO-8859-1");
		}
		if (isGzip)
			bts = GZIPUtils.unzip(bts);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bts);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			return objectInputStream.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	/**
	 * 初始化链接
	 * 
	 * @param conf
	 * @param lstSourcePO
	 * @throws Exception
	 */
	public static Object getSourceSystem(FTPDataSourcePO ds) {
		try {
			String protocol = ds.getProtocol();
			String ip = ds.getIp();
			String port = ds.getPort();
			String username = ds.getUserName();
			String password = ds.getPassword();

			if (Contant.PROTOCOL_LOCAL.equals(protocol)) {
				return "";
			} else if (Contant.PROTOCOL_LOCALHDFS.equals(protocol)) {
				return HDFSUtils.getFileSystem(ip, port);
			} else if (Contant.PROTOCOL_REMOREHDFS.equals(protocol)) {
				return HDFSUtils.getFileSystem(ip, port);
			} else if (Contant.PROTOCOL_FTP.equals(protocol)) {
				IFileTransfer ftp = new FTPTransfer();
				SourceUtil.initFTP(ftp, ip, port, username, password, ds);
				return ftp;
			} else if (Contant.PROTOCOL_SFTP.equals(protocol)) {
				IFileTransfer ftp = new SFTPTransfer();
				SourceUtil.initFTP(ftp, ip, port, username, password, ds);
				return ftp;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return null;
	}

	public static void initFTP(IFileTransfer ftp, String ip, String port, String username, String password,
			FTPDataSourcePO ds) throws Exception {
		String connectMax = Convert.toInt(ds.get(MRConfiguration.CONNECT_MAX, "3"), 3) + "";
		String connectTime = Convert.toInt(ds.get(MRConfiguration.CONNECT_TIME, "3000"), 3000) + "";
		String connectTimeOut = Convert.toInt(ds.get(MRConfiguration.CONNECT_TIMEOUT, "30000"), 30000) + "";
		String ftpConnectMode = ds.get(MRConfiguration.FTP_CONNECT_MODE, "port");// ftp.connect.mode
		if (ftpConnectMode.equals("")) {
			ftpConnectMode = "port";
		}
		String encodeType = ds.get(MRConfiguration.FTP_CONNECT_ENCODE, "GBK");
		if (encodeType.equals("")) {
			encodeType = "GBK";
		}
		String transtype = ds.get(MRConfiguration.FTP_CONNECT_TRANSTYPE, "false");
		if (transtype.equals("")) {
			transtype = "false";
		}
		Map<String, Object> mapInit = new HashMap<String, Object>();
		mapInit.put(FTPTransfer.USERNAME, username);
		mapInit.put(FTPTransfer.PASSWORD, password);
		mapInit.put(FTPTransfer.REMOTE_HOST, ip);
		mapInit.put(FTPTransfer.REMOTE_POST, port);
		mapInit.put(FTPTransfer.CONNECT_MAX, connectMax);
		mapInit.put(FTPTransfer.CONNECT_TIME, connectTime);
		mapInit.put(FTPTransfer.ADVANCED_CONTROLENCODING, encodeType);
		mapInit.put(FTPTransfer.ADVANCED_CONNECTMODE, ftpConnectMode);
		mapInit.put(FTPTransfer.ADVANCED_TRANSTYPE, transtype);
		mapInit.put(FTPTransfer.CONNECT_TIMEOUT, connectTimeOut);
		MRLog.systemOut("connecting ftp ,params=" + mapInit);
		ftp.init(mapInit);
		try {
			ftp.connectServer();
		} catch (Exception e) {
			MRLog.systemOut("ftp init error:" + username + "@" + ip + ":" + port + "");
			e.printStackTrace();
			throw new Exception(username + "@" + ip + ":" + port);
		}
	}

	/**
	 * 验证协议是否正确
	 * 
	 * @param address
	 * @return
	 * @throws Exception
	 */
	public static void validateProtocol(List<DBSourceRecordPO> dbValues) throws Exception {
		for (DBSourceRecordPO spo : dbValues) {
			FTPDataSourcePO ftpSource = spo.getInputDatasource();
			if (null == ftpSource) {
				throw new Exception("FTPPath in protocol is null.");
			}

			String protocol = ftpSource.getProtocol();
			if (!(Contant.PROTOCOL_LOCAL.equals(protocol) || Contant.PROTOCOL_LOCALHDFS.equals(protocol) ||
					Contant.PROTOCOL_REMOREHDFS.equals(protocol) || Contant.PROTOCOL_FTP.equals(protocol) || Contant.PROTOCOL_SFTP
						.equals(protocol))) {
				throw new Exception("FTPPath in protocol'type is error.");
			}

			String inip = ftpSource.getIp();
			int inport = StringUtil.stringToInt(ftpSource.getPort(), -1);
			if (!Contant.PROTOCOL_LOCAL.equals(protocol) &&
					(null == inip || inip.trim().length() <= 0 || inport < 0 || inport > 65535)) {
				throw new Exception("FTPPath in protocol ip or port is error." + " ip=" + inip + ", port=" +
						ftpSource.getPort());
			}

			FTPDataSourcePO outftpSource = spo.getOutputDatasource();
			if (null == outftpSource) {
				throw new Exception("FTPPath out protocol is null.");
			}

			String outProtocol = outftpSource.getProtocol();
			if (!(Contant.PROTOCOL_LOCAL.equals(outProtocol) || Contant.PROTOCOL_LOCALHDFS.equals(outProtocol) ||
					Contant.PROTOCOL_REMOREHDFS.equals(outProtocol) || Contant.PROTOCOL_FTP.equals(outProtocol) || Contant.PROTOCOL_SFTP
						.equals(outProtocol))) {
				throw new Exception("FTPPath out protocol'type is error.");
			}

			String outip = ftpSource.getIp();
			int outport = StringUtil.stringToInt(outftpSource.getPort(), -1);
			if (!(Contant.PROTOCOL_LOCAL.equals(protocol) || Contant.PROTOCOL_LOCAL.equals(outProtocol)) &&
					(null == outip || outip.trim().length() <= 0 || outport < 0 || outport > 65535)) {
				throw new Exception("FTPPath out protocol ip or port is error." + " ip=" + outip + ", port=" +
						outftpSource.getPort());
			}
		}
	}

	/**
	 * 验证输入路径的协议和地址是否正确
	 * 
	 * @param address
	 * @return
	 * @throws Exception
	 */
	public static void validateInputProtocolAndPath(List<DBSourceRecordPO> address) throws Exception {
		Map<String, List<String>> mapPaths = new HashMap<String, List<String>>();
		for (DBSourceRecordPO spo : address) {
			FTPDataSourcePO ftpSource = spo.getInputDatasource();
			if (null == ftpSource) {
				throw new Exception("FTPPath in protocol is null.");
			}

			String protocol = spo.getInputDatasource().getProtocol();
			if (!(Contant.PROTOCOL_LOCAL.equals(protocol) || Contant.PROTOCOL_LOCALHDFS.equals(protocol) ||
					Contant.PROTOCOL_REMOREHDFS.equals(protocol) || Contant.PROTOCOL_FTP.equals(protocol) || Contant.PROTOCOL_SFTP
						.equals(protocol))) {
				throw new Exception("FTPPath in protocol'type is error.");
			}

			String inputRootPath = SourceUtil.unitePath(spo.getInputPath());
			spo.setInputPath(inputRootPath);

			List<String> lstPath = mapPaths.get(spo.getInputUniqueKey());
			if (null == lstPath) {
				lstPath = new ArrayList<String>();
				mapPaths.put(spo.getInputUniqueKey(), lstPath);
			}

			if (lstPath.contains(inputRootPath)) {
				throw new Exception("FTPPath is error, inputpath is not unique; PATH:" + spo.toString());
			}

			lstPath.add(inputRootPath);
		}
	}

	/**
	 * 验证输出路径的协议和地址是否正确
	 * 
	 * @param address
	 * @return
	 * @throws Exception
	 */
	public static void validateOutputProtocolAndPath(List<DBSourceRecordPO> address) throws Exception {
		Map<String, List<String>> mapPaths = new HashMap<String, List<String>>();
		for (DBSourceRecordPO spo : address) {
			FTPDataSourcePO ftpSource = spo.getOutputDatasource();
			if (null == ftpSource) {
				throw new Exception("FTPPath out protocol is null.");
			}

			String protocol = spo.getOutputDatasource().getProtocol();
			if (!(Contant.PROTOCOL_LOCAL.equals(protocol) || Contant.PROTOCOL_LOCALHDFS.equals(protocol) ||
					Contant.PROTOCOL_REMOREHDFS.equals(protocol) || Contant.PROTOCOL_FTP.equals(protocol) || Contant.PROTOCOL_SFTP
						.equals(protocol))) {
				throw new Exception("FTPPath out protocol'type is error.");
			}

			String OutputRootPath = SourceUtil.unitePath(spo.getOutputPath());
			spo.setOutputPath(OutputRootPath);

			List<String> lstPath = mapPaths.get(spo.getOutputUniqueKey());
			if (null == lstPath) {
				lstPath = new ArrayList<String>();
				mapPaths.put(spo.getOutputUniqueKey(), lstPath);
			}

			if (lstPath.contains(OutputRootPath)) {
				throw new Exception("FTPPath is error, outputpath is not unique; PATH:" + spo.toString());
			}

			lstPath.add(OutputRootPath);
		}
	}

	/**
	 * 初始化输入服务
	 * 
	 * @param conf
	 * @param dbValues
	 */
	public static Object initServer(FTPDataSourcePO ds) {
		Object sourceSystem = SourceUtil.getSourceSystem(ds);
		return sourceSystem;
	}

	// /**
	// * 初始化输入服务
	// * @param conf
	// * @param dbValues
	// */
	// public static void initInputServer(JobConf conf, List<DBSourceRecordPO>
	// dbValues) {
	// Map<String, Object> mapSourceSystem = new HashMap<String, Object>();
	// for (DBSourceRecordPO dbsrp : dbValues) {
	// FTPDataSourcePO ds = dbsrp.getInputDatasource();
	// // 获取资源
	// Object sourceSystem = mapSourceSystem.get(ds.getSourceSystemOnlyFlag());
	// if (!mapSourceSystem.containsKey(ds.getSourceSystemOnlyFlag()) && null ==
	// sourceSystem){
	// sourceSystem = SourceUtil.getSourceSystem(conf, ds.getProtocol(),
	// ds.getIp(),
	// ds.getPort(), ds.getUserName(), ds.getPassword());
	// mapSourceSystem.put(ds.getSourceSystemOnlyFlag(), sourceSystem);
	// }
	//
	// if (!Contant.PROTOCOL_LOCAL.equals(ds.getProtocol()) && null ==
	// sourceSystem){
	// dbsrp.setInputStatus(0);
	// }
	// dbsrp.setInputSourceSystem(sourceSystem);
	// }
	// }
	//
	/**
	 * 初始化输出服务
	 * 
	 * @param conf
	 * @param dbValues
	 */
	public static void initOutputServer(JobConf conf, List<DBSourceRecordPO> dbValues) {
		Map<String, Object> mapSourceSystem = new HashMap<String, Object>();
		for (DBSourceRecordPO dbsrp : dbValues) {
			FTPDataSourcePO ds = dbsrp.getOutputDatasource();
			// 获取资源
			Object sourceSystem = mapSourceSystem.get(ds.getSourceSystemOnlyFlag());
			if (!mapSourceSystem.containsKey(ds.getSourceSystemOnlyFlag()) && null == sourceSystem) {
				sourceSystem = SourceUtil.initServer(ds);
				mapSourceSystem.put(ds.getSourceSystemOnlyFlag(), sourceSystem);
			}
			if (!Contant.PROTOCOL_LOCAL.equals(ds.getProtocol()) && null == sourceSystem) {
				dbsrp.setOutputStatus(0);
			}

			dbsrp.setOutputSourceSystem(sourceSystem);
		}
	}

	/**
	 * 获取输入的所有有效文件
	 * 
	 * @param dbValues
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws Exception
	 */
	public static List<FileAttributePO> getEffectiveInputAllFilePath(DBSourceRecordPO dbsrp,
			Map<String, Set<String>> mapSourceInputpath) throws ClassNotFoundException, SQLException, Exception {
		// 获取所有目录
		if (DBSourceRecordPO.CONSTANT_INPUT_FILELST_TYPE_FTP == dbsrp.getInputFilelstType()) {
			Object sourceSystem = dbsrp.getInputSourceSystem();
			if (null == sourceSystem || !dbsrp.isInputRight()) {// 资源不能访问时
				return new ArrayList<FileAttributePO>();
			}

			// 添加root目录下所有文件
			return SourceUtil.getFtpInputSourcePath(dbsrp, sourceSystem, mapSourceInputpath);
		} else if (DBSourceRecordPO.CONSTANT_INPUT_FILELST_TYPE_DB == dbsrp.getInputFilelstType()) {
			Object sourceSystem = dbsrp.getInputSourceSystem();
			if (null == sourceSystem || !dbsrp.isInputRight()) {// 资源不能访问时
				return new ArrayList<FileAttributePO>();
			}

			// 添加查询的所有文件
			return SourceUtil.getJdbcInputPath(dbsrp, sourceSystem);
		}

		return new ArrayList<FileAttributePO>();
	}

	/**
	 * 获取输入的所有有效文件
	 * 
	 * @param dbValues
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws Exception
	 */
	public static List<InputSourceRecordPO> getEffectiveInputAllFilePaths(List<InputSourceRecordPO> lstInputAllRecord)
			throws ClassNotFoundException, SQLException, Exception {
		List<InputSourceRecordPO> lstRemove = new ArrayList<InputSourceRecordPO>();
		for (InputSourceRecordPO isrpflag : lstInputAllRecord) {
			for (InputSourceRecordPO isrp : lstInputAllRecord) {
				if (isrpflag.getDataSource().getSourceSystemOnlyFlag()
						.equals(isrp.getDataSource().getSourceSystemOnlyFlag()) &&
						isrpflag.getRootPath().length() != isrp.getRootPath().length() &&
						isrpflag.getRootPath().startsWith(isrp.getRootPath()) &&
						isrp.getPath().startsWith(isrpflag.getRootPath())) {
					lstRemove.add(isrp);
				}
			}
		}

		for (InputSourceRecordPO isrp : lstRemove) {
			lstInputAllRecord.remove(isrp);
		}

		return lstInputAllRecord;
	}

	/**
	 * 获取所有输出的有效资源对象
	 * 
	 * @param dbValues
	 * @return
	 */
	public static List<OutputSourceRecordPO> getEffectiveOutputSource(List<DBSourceRecordPO> dbValues) {
		List<OutputSourceRecordPO> lstOutput = new ArrayList<OutputSourceRecordPO>();
		for (DBSourceRecordPO dbsr : dbValues) {
			if (dbsr.isOutputRight()) {
				OutputSourceRecordPO osrp = new OutputSourceRecordPO();
				osrp.setColId(dbsr.getColId());
				osrp.setDataSource(dbsr.getOutputDatasource());
				osrp.setMovePath(dbsr.getOutputMovePath());
				osrp.setOutputSourceSystem(dbsr.getOutputSourceSystem());
				osrp.setRenameRule(dbsr.getOutputRenameRule());
				osrp.setRename(dbsr.getOutputRename());
				osrp.setRootPath(dbsr.getOutputPath());
				lstOutput.add(osrp);
			}
		}

		return lstOutput;
	}

	// /**
	// * 验证输入路径是否存在
	// * @param conf
	// * @param values
	// */
	// public static void validateInputRootPath(Configuration conf,
	// List<DBSourceRecordPO> values) {
	// // 判断文件是否存在.
	// for (DBSourceRecordPO dbsr : values) {
	// String protocol = dbsr.getInputDatasource().getProtocol();
	// String rootPath = dbsr.getInputPath();
	// Object objSourceSystem = dbsr.getInputSourceSystem();
	// boolean status = false;
	// if (Contant.PROTOCOL_LOCAL.equals(protocol)) {
	// File file = new File(rootPath);
	// if (file.exists()) {
	// status = true;
	// }
	// } else if (null != objSourceSystem &&
	// Contant.PROTOCOL_REMOREHDFS.equals(protocol) && objSourceSystem
	// instanceof FileSystem) {
	// FileSystem fileSystem = (FileSystem) objSourceSystem;
	// try {
	// if (fileSystem.exists(new Path(rootPath))) {
	// status = true;
	// }
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// } else if (null != objSourceSystem &&
	// Contant.PROTOCOL_FTP.equals(protocol) && objSourceSystem instanceof
	// IFileTransfer) {
	// IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
	// if (fileTransfer.exist(rootPath)) {
	// status = true;
	// }
	// } else if (null != objSourceSystem &&
	// Contant.PROTOCOL_SFTP.equals(protocol) && objSourceSystem instanceof
	// IFileTransfer) {
	// IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
	// if (fileTransfer.exist(rootPath)) {
	// status = true;
	// }
	// }
	//
	// dbsr.setInputStatus(status?-1:1);
	// }
	// }

	/**
	 * 验证输出路径是否存在
	 * 
	 * @param conf
	 * @param values
	 */
	public static void validateOutputRootPath(Configuration conf, DBSourceRecordPO sourcePO) {
		// 判断文件是否是文件.
		FTPDataSourcePO ftpds = sourcePO.getOutputDatasource();
		String protocol = ftpds.getProtocol();
		String rootPath = sourcePO.getOutputPath();
		Object objSourceSystem = sourcePO.getOutputSourceSystem();
		boolean status = true;
		boolean isException = false;
		if (Contant.PROTOCOL_LOCAL.equals(protocol)) {
			File file = new File(rootPath);
			if (file.exists() && file.isFile()) {
				status = false;
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_REMOREHDFS.equals(protocol) &&
				objSourceSystem instanceof FileSystem) {
			FileSystem fileSystem = (FileSystem) objSourceSystem;
			Path path = new Path(rootPath);
			try {
				if (fileSystem.exists(new Path(rootPath)) && fileSystem.isFile(path)) {
					status = false;
				}
			} catch (IOException e) {
				isException = true;
				e.printStackTrace();
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_FTP.equals(protocol) &&
				objSourceSystem instanceof IFileTransfer) {
			IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
			try {
				if (fileTransfer.exist(rootPath) && fileTransfer.isFile(rootPath)) {
					status = false;
				}
			} catch (Exception e) {
				isException = true;
				e.printStackTrace();
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_SFTP.equals(protocol) &&
				objSourceSystem instanceof IFileTransfer) {
			IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
			try {
				if (fileTransfer.exist(rootPath) && fileTransfer.isFile(rootPath)) {
					status = false;
				}
			} catch (Exception e) {
				isException = true;
				e.printStackTrace();
			}
		}

		if (isException) {
			sourcePO.setOutputStatus(0);
		} else {
			sourcePO.setOutputStatus(status ? -1 : 1);
		}
	}

	// /**
	// * 获取FTP下的所有输入文件的列表
	// *
	// * @param sourceSystem2
	// * @param sourceInputPO
	// * @return
	// */
	// public static List<InputSourceRecordPO> getFtpPaths(DBSourceRecordPO sp,
	// Object sourceSystem) {
	// List<InputSourceRecordPO> tempAll = new ArrayList<InputSourceRecordPO>();
	// String rootPath = sp.getInputPath();
	// Pattern pattern = sp.getInputFileRulePattern();
	// FTPDataSourcePO ftpsp = sp.getInputDatasource();
	// String protocol = ftpsp.getProtocol();
	//
	// // 获取文件列表
	// Map<FileAttributePO, String> mapTempPath = null;
	// if (Contant.PROTOCOL_LOCAL.equals(protocol)) {
	// mapTempPath = SourceUtil.getPathLocal(rootPath, pattern);
	// } else if (Contant.PROTOCOL_LOCALHDFS.equals(protocol) && sourceSystem
	// instanceof FileSystem) {
	// mapTempPath = SourceUtil.getPathLocalHDFS((FileSystem) sourceSystem, new
	// String[]{rootPath}, pattern);
	// } else if (Contant.PROTOCOL_REMOREHDFS.equals(protocol) && sourceSystem
	// instanceof FileSystem) {
	// mapTempPath = SourceUtil.getPathLocalHDFS((FileSystem) sourceSystem, new
	// String[]{rootPath}, pattern);
	// } else if (Contant.PROTOCOL_FTP.equals(protocol) && sourceSystem
	// instanceof IFileTransfer) {
	// mapTempPath = SourceUtil.getPathFTP((IFileTransfer) sourceSystem, new
	// String[]{rootPath}, pattern);
	// } else if (Contant.PROTOCOL_SFTP.equals(protocol) && sourceSystem
	// instanceof IFileTransfer) {
	// mapTempPath = SourceUtil.getPathFTP((IFileTransfer) sourceSystem, new
	// String[]{rootPath}, pattern);
	// }
	//
	// if (null == mapTempPath) {
	// return tempAll;
	// }
	//
	// for (FileAttributePO fap : mapTempPath.keySet()) {
	// InputSourceRecordPO ips = new InputSourceRecordPO();
	// ips.setDataSource(sp.getInputDatasource());
	// ips.setPath(fap.getPath());
	// ips.setColId(sp.getColId());
	// ips.setDotype(sp.getInputDotype());
	// ips.setFileRule(sp.getInputFileRule());
	// ips.setMovePath(sp.getInputMovePath());
	// ips.setRootPath(sp.getInputPath());
	// ips.setRenameRule(sp.getInputRenameRule());
	// ips.setRename(sp.getInputRename());
	// ips.setDataSource(sp.getInputDatasource());
	// try {
	// ips.setLastModiyTime(StringUtil.longToString(fap.getLastModifyTime(),
	// StringUtil.DATE_FORMAT_TYPE1));
	// } catch (ParseException e) {
	// ips.setLastModiyTime("-1");
	// }
	// ips.setSize(fap.getSize());
	// ips.setSourceSystem(sourceSystem);
	// tempAll.add(ips);
	// }
	//
	// return tempAll;
	// }
	//
	/**
	 * 获取FTP下的所有输入文件的列表
	 * 
	 * @param sourceSystem2
	 * @param sourceInputPO
	 * @return
	 */
	public static List<FileAttributePO> getFtpInputSourcePath(DBSourceRecordPO sp, Object sourceSystem,
			Map<String, Set<String>> mapSourceInputpath) {
		List<FileAttributePO> tempAll = new ArrayList<FileAttributePO>();
		String rootPath = sp.getInputPath();
		Pattern pattern = sp.getInputFileRulePattern();
		FTPDataSourcePO ftpsp = sp.getInputDatasource();
		String protocol = ftpsp.getProtocol();
		Set<String> lstRootPath = mapSourceInputpath.get(ftpsp.getSourceSystemOnlyFlag());

		// 获取文件列表
		Map<FileAttributePO, String> mapTempPath = null;
		if (Contant.PROTOCOL_LOCAL.equals(protocol)) {
			mapTempPath = SourceUtil.getPathLocal(rootPath, pattern, lstRootPath);
		} else if (Contant.PROTOCOL_LOCALHDFS.equals(protocol) && sourceSystem instanceof FileSystem) {
			mapTempPath = SourceUtil.getPathLocalHDFS((FileSystem) sourceSystem, new String[] { rootPath }, pattern,
					lstRootPath);
		} else if (Contant.PROTOCOL_REMOREHDFS.equals(protocol) && sourceSystem instanceof FileSystem) {
			mapTempPath = SourceUtil.getPathLocalHDFS((FileSystem) sourceSystem, new String[] { rootPath }, pattern,
					lstRootPath);
		} else if (Contant.PROTOCOL_FTP.equals(protocol) && sourceSystem instanceof IFileTransfer) {
			mapTempPath = SourceUtil.getPathFTP((IFileTransfer) sourceSystem, new String[] { rootPath }, pattern,
					lstRootPath);
		} else if (Contant.PROTOCOL_SFTP.equals(protocol) && sourceSystem instanceof IFileTransfer) {
			mapTempPath = SourceUtil.getPathFTP((IFileTransfer) sourceSystem, new String[] { rootPath }, pattern,
					lstRootPath);
		}

		if (null == mapTempPath) {
			return tempAll;
		}

		for (FileAttributePO fap : mapTempPath.keySet()) {
			tempAll.add(fap);
		}

		return tempAll;
	}

	/**
	 * 重新设置文件最后修改时间和文件大小
	 * 
	 * @param inputSourceSystem
	 * @param inputSourceProtocol
	 * @param inputPath
	 * @param isrp
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws Exception
	 */
	public static void setInputFileLastModify(Object inputSourceSystem, String inputSourceProtocol,
			InputSourceRecordPO isrp) throws FileNotFoundException, IOException, Exception {
		long lastModifyTime = -1;
		long filesize = -1;
		String inputPath = isrp.getPath();
		// 获取输入流
		if (Contant.PROTOCOL_LOCAL.equals(inputSourceProtocol)) {
			File file = new File(inputPath);
			if (file.exists()) {
				lastModifyTime = file.lastModified();
				filesize = file.length();
			}
		} else if (Contant.PROTOCOL_LOCALHDFS.equals(inputSourceProtocol) && inputSourceSystem instanceof FileSystem) {
			FileSystem fs = ((FileSystem) inputSourceSystem);
			Path path = new Path(inputPath);
			if (fs.exists(path)) {
				FileStatus fileStatus = fs.getFileStatus(path);
				lastModifyTime = fileStatus.getModificationTime();
				filesize = fileStatus.getLen();
			}
		} else if (Contant.PROTOCOL_REMOREHDFS.equals(inputSourceProtocol) && inputSourceSystem instanceof FileSystem) {
			lastModifyTime = ((FileSystem) inputSourceSystem).getFileStatus(new Path(inputPath)).getModificationTime();
			filesize = ((FileSystem) inputSourceSystem).getFileStatus(new Path(inputPath)).getLen();
		} else if (Contant.PROTOCOL_FTP.equals(inputSourceProtocol) && inputSourceSystem instanceof IFileTransfer) {
			Map<FileAttribute, String> mapPath = ((IFileTransfer) inputSourceSystem)
					.directoryMap(new String[] { inputPath });
			for (FileAttribute fa : mapPath.keySet()) {
				filesize = fa.getSize();
				lastModifyTime = fa.getLastModifyTime();
				break;
			}
		} else if (Contant.PROTOCOL_SFTP.equals(inputSourceProtocol) && inputSourceSystem instanceof IFileTransfer) {
			Map<FileAttribute, String> mapPath = ((IFileTransfer) inputSourceSystem)
					.directoryMap(new String[] { inputPath });
			for (FileAttribute fa : mapPath.keySet()) {
				filesize = fa.getSize();
				lastModifyTime = fa.getLastModifyTime();
				break;
			}
		}

		String lastTime = "-1";
		try {
			lastTime = StringUtil.longToString(lastModifyTime, StringUtil.DATE_FORMAT_TYPE1);
		} catch (ParseException e) {
			e.printStackTrace();
			lastTime = "-1";
		}

		isrp.setLastModiyTime(lastTime);
		isrp.setSize(filesize);
	}

	/**
	 * 获取jdbc下的所有输入文件的列表
	 * 
	 * @param sourceSystem2
	 * @param sourceInputPO
	 * @return
	 */
	public static List<FileAttributePO> getJdbcInputPath(DBSourceRecordPO dbsrp, Object sourceSystem)
			throws ClassNotFoundException, SQLException, Exception {
		List<FileAttributePO> tempAll = new ArrayList<FileAttributePO>();
		Map<String, String> param = dbsrp.getInputFilelstDatasourceParam();
		System.out.println("mr.mapred.jdbc.url=" + param.get("mr.mapred.jdbc.url"));
		System.out.println("mr.mapred.jdbc.username=" + param.get("mr.mapred.jdbc.username"));
		System.out.println("mr.mapred.jdbc.password=" + param.get("mr.mapred.jdbc.password"));
		JDBCUtil jdbc = null;
		if (param.containsKey(DBConfiguration.RDB_DATA_SOURCE_DRIVER)) {
			jdbc = new JDBCUtil(param.get(DBConfiguration.RDB_DATA_SOURCE_DRIVER), param.get("mr.mapred.jdbc.url"),
					param.get("mr.mapred.jdbc.username"), param.get("mr.mapred.jdbc.password"));
		} else {
			jdbc = new JDBCUtil(param.get("mr.mapred.jdbc.url"), param.get("mr.mapred.jdbc.username"),
					param.get("mr.mapred.jdbc.password"));
		}
		ResultSet result = jdbc.query(dbsrp.getInputQuerySql());
		while (result.next()) {
			FileAttributePO fap = new FileAttributePO();
			fap.setPath(result.getString(1));
			fap.setLastModifyTime(result.getLong(2));
			fap.setSize(result.getLong(3));
			tempAll.add(fap);
		}

		jdbc.close();
		return tempAll;
	}

	/**
	 * 获取jdbc下的所有输入文件的列表
	 * 
	 * @param sourceSystem2
	 * @param sourceInputPO
	 * @return
	 */
	public static List<InputSourceRecordPO> getJdbcPath(DBSourceRecordPO dbsrp, Object sourceSystem)
			throws ClassNotFoundException, SQLException, Exception {
		List<InputSourceRecordPO> tempAll = new ArrayList<InputSourceRecordPO>();
		Map<String, String> param = dbsrp.getInputFilelstDatasourceParam();
		JDBCUtil jdbc = new JDBCUtil(param.get("mr.mapred.jdbc.url"), param.get("mr.mapred.jdbc.username"),
				param.get("mr.mapred.jdbc.password"));
		ResultSet result = jdbc.query(dbsrp.getInputQuerySql());
		while (result.next()) {
			InputSourceRecordPO ips = new InputSourceRecordPO();
			ips.setDataSource(dbsrp.getInputDatasource());
			ips.setPath(result.getString(1));
			ips.setColId(dbsrp.getColId());
			ips.setDotype(dbsrp.getInputDotype());
			ips.setFileRule(dbsrp.getInputFileRule());
			ips.setMovePath(dbsrp.getInputMovePath());
			ips.setRootPath(dbsrp.getInputPath());
			ips.setRenameRule(dbsrp.getInputRenameRule());
			ips.setRename(dbsrp.getInputRename());
			ips.setDataSource(dbsrp.getInputDatasource());

			ips.setInputSourceSystem(sourceSystem);
			tempAll.add(ips);
		}

		jdbc.close();
		return tempAll;
	}

	/**
	 * 关闭输入资源
	 * 
	 * @param tempSourceSystemMap
	 * @throws IOException
	 */
	public static void closeInputSourceSystem(List<DBSourceRecordPO> lstDBSource) {
		for (DBSourceRecordPO dbsrp : lstDBSource) {
			Object inputObj = dbsrp.getInputSourceSystem();
			Object outputObj = dbsrp.getOutputSourceSystem();
			close(inputObj);
			close(outputObj);
			dbsrp.setOutputSourceSystem(null);
			dbsrp.setInputSourceSystem(null);
		}
	}

	/**
	 * 关闭
	 * 
	 * @param obj
	 */
	public static void close(Object obj) {
		if (null == obj) {
			return;
		}
		if (obj instanceof FileSystem) {
			FileSystem fileSystem = (FileSystem) obj;
			try {
				fileSystem.close();
			} catch (IOException e) {
			}
		} else if (obj instanceof IFileTransfer) {
			IFileTransfer fileTransfer = (IFileTransfer) obj;
			try {
				fileTransfer.close();
			} catch (FTPException e) {
			} catch (IOException e) {
			}
		}
	}

	/**
	 * 关闭输入资源
	 * 
	 * @param tempSourceSystemMap
	 * @throws IOException
	 */
	public static void closeOutputSourceSystem(List<DBSourceRecordPO> lstDBSource) {
		for (DBSourceRecordPO dbsrp : lstDBSource) {
			Object obj = dbsrp.getOutputSourceSystem();
			if (null == obj) {
				continue;
			}
			if (obj instanceof FileSystem) {
				FileSystem fileSystem = (FileSystem) obj;
				try {
					fileSystem.close();
				} catch (IOException e) {
				}
			} else if (obj instanceof IFileTransfer) {
				IFileTransfer fileTransfer = (IFileTransfer) obj;
				try {
					fileTransfer.close();
				} catch (FTPException e) {
				} catch (IOException e) {
				}
			}
			dbsrp.setOutputSourceSystem(null);
		}
	}

	/**
	 * 初始化记录FTP文件日志信息数据库中(文件的创建日志默认为空，状态默认为0，表示初始化)
	 * 
	 * @param ftpconf
	 * @param lstUpdateFTPLogPO
	 * @param lstInsertFTPLogPO
	 * 
	 * @param lstInputRecord
	 * @param jobId
	 * @param splits
	 */
	public static void initFTPLogInfo(FTPConfiguration ftpconf, InputSourceRecordPO isrp,
			List<OutputSourceRecordPO> lstOSRP, List<FTPLogPO> lstInsertFTPLogPO, List<FTPLogPO> lstUpdateFTPLogPO) {
		// 初始记录文件日志信息到数据库(文件下载或者上传日志输入的总日志、用于判断文件是否下载过的日志).
		int colId = isrp.getColId();
		String inputFileInfo = isrp.getSourceOnlyFlag();
		for (OutputSourceRecordPO outputRecord : lstOSRP) {
			String outputFileInfo = outputRecord.getSourceOnlyFlag();
			long colFileDetailId = outputRecord.getColFileDetailId();
			// 预先判断源文件是否已经下载到目标目录中,若已存在,则继续
			long colLogId = outputRecord.getColLogId();
			FTPLogPO ftpLogPO = MRLog.getInstance().getJobRemoteFileCreateDate(colId, colLogId, inputFileInfo,
					outputFileInfo);
			if (null == ftpLogPO) {
				FTPLogPO log = new FTPLogPO();
				log.setColId(colId);
				log.setColLogId(colLogId);
				log.setRunDateTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
				log.setInputFileMsg(inputFileInfo);
				log.setOutputFileMsg(outputFileInfo);
				log.setInputFileLastModifyTime(isrp.getLastModiyTime());
				log.setStatus(FTPLogPO.STATUS_INIT);
				log.setColFileDetailId(colFileDetailId);
				lstInsertFTPLogPO.add(log);

				continue;
			}

			if (ftpconf.getFtpDownCollectForceRepeat()) {// 更新至重新下载
				FTPLogPO log = new FTPLogPO();
				log.setColId(colId);
				log.setColLogId(colLogId);
				log.setRunDateTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
				log.setInputFileMsg(inputFileInfo);
				log.setOutputFileMsg(outputFileInfo);
				log.setInputFileLastModifyTime(isrp.getLastModiyTime());
				log.setStatus(FTPLogPO.STATUS_REPEAT);
				log.setColFileDetailId(colFileDetailId);
				lstUpdateFTPLogPO.add(log);
				continue;
			}

			// 上次不成功，或者成功但文件更新过，则修改为初始化
			String fileDate = ftpLogPO.getInputFileLastModifyTime();
			if ((ftpLogPO.getStatus() != FTPLogPO.STATUS_SUCCESS) ||
					(null != fileDate && !fileDate.equals(isrp.getLastModiyTime())) &&
					!(ftpconf.getFtpDownFail() || ftpconf.getFtpUpFail())) {
				FTPLogPO log = new FTPLogPO();
				log.setColId(colId);
				log.setColLogId(colLogId);
				log.setRunDateTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
				log.setInputFileMsg(inputFileInfo);
				log.setOutputFileMsg(outputFileInfo);
				log.setInputFileLastModifyTime(isrp.getLastModiyTime());
				log.setStatus(FTPLogPO.STATUS_INIT);
				log.setColFileDetailId(colFileDetailId);
				outputRecord.setTranSuccess(false);
				lstUpdateFTPLogPO.add(log);
			}
		}
	}

	/**
	 * 
	 * @param isrp
	 * @param lstMRColDetailLog
	 */
	public static void initColFileLog(InputSourceRecordPO isrp, List<OutputSourceRecordPO> lstOsr,
			List<MRColDetailLog> lstMRColDetailLog) {
		for (OutputSourceRecordPO osrp : lstOsr) {
			MRColDetailLog filelog = new MRColDetailLog();
			filelog.setColId(isrp.getColId());
			filelog.setColLogId(osrp.getColLogId());
			filelog.setFileSize(isrp.getSize());
			filelog.setInputFileName(StringUtil.getFileName(isrp.getPath()));
			filelog.setInputRename(isrp.getNewName());
			filelog.setInputPath(isrp.getPath().substring(0, isrp.getPath().lastIndexOf("/")));
			filelog.setIsDoinputfiletype(isrp.getDotype());
			filelog.setMoveInputPath(isrp.getMovePath());
			filelog.setStartTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
			filelog.setStatus(MRColDetailLog.STATUS_INIT);
			if (osrp.getColFileDetailId() != 0) {
				continue;
			}
			filelog.setIsMoveOutput((osrp.getMovePath() == null || osrp.getMovePath().trim().length() <= 0) ? MRColDetailLog.ISMOVEOUTPUT_NO
					: MRColDetailLog.ISMOVEOUTPUT_YES);
			filelog.setIsOutputRename((osrp.getRename() == null || osrp.getRename().trim().length() <= 0) ? MRColDetailLog.ISOUTPUTRENAME_NO
					: MRColDetailLog.ISOUTPUTRENAME_YES);
			filelog.setMoveOutputPath(osrp.getMovePath());
			filelog.setOutputPath(osrp.getPath().substring(0, osrp.getPath().lastIndexOf("/")));
			filelog.setOutputRename(osrp.getNewName());
			long id = MRLog.getInstance().getColFileDetailLog();
			filelog.setId(id);
			osrp.setColFileDetailId(id);
			lstMRColDetailLog.add(filelog);
		}
	}

	/**
	 * 初始化采集任务日志
	 * 
	 * @param lstInputRecord
	 * @return
	 */
	public static long initColTaskLog(List<InputSourceRecordPO> lstInputRecord, String queueName, String jobCmd) {
		// 初始化采集任务日志
		if (lstInputRecord.size() > 0) {
			long fileTotalsize = 0;
			int fileNum = 0;
			for (InputSourceRecordPO isrp : lstInputRecord) {
				fileTotalsize += isrp.getSize();
				fileNum++;
			}

			// 记录新的日志信息
			InputSourceRecordPO isrp = lstInputRecord.get(0);
			MRColFileLog log = new MRColFileLog();
			log.setColId(isrp.getColId());
			log.setFileNum(fileNum);
			log.setFileTotalsize(fileTotalsize);
			log.setStartTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
			log.setStatus(MRColFileLog.STATUS_INIT);
			log.setQueueName(queueName);
			log.setJobCmd(jobCmd);
			MRLog.getInstance().colFileLog(log);
			return log.getColLogid();
		}
		return -1l;
	}

	/**
	 * 重新执行的日志状态更新
	 * 
	 * @param lstInputRecord
	 */
	public static void updateColTaskLogInit(List<InputSourceRecordPO> lstInputRecord) {
		// 重新执行的日志状态更新
		Set<Long> colLogIdLst = new HashSet<Long>();
		for (InputSourceRecordPO inputSourceRecordPO : lstInputRecord) {
			for (OutputSourceRecordPO out : inputSourceRecordPO.getLstOutputSourceRecord()) {
				colLogIdLst.add(out.getColLogId());
			}
		}

		for (Long colLogId : colLogIdLst) {
			updateColTaskLogSomeTimeInit(colLogId);
		}
	}

	/**
	 * 重新执行的日志状态更新
	 * 
	 * @param lstInputRecord
	 */
	public static void updateColTaskLogSomeTimeInit(long colLogId) {
		MRLog.systemOut("<col_log_id>" + colLogId + "</col_log_id>");
		MRColFileLog log = new MRColFileLog();
		log.setColLogid(colLogId);
		log.setStatus(MRColFileLog.STATUS_RERUN);
		log.setStartTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		MRLog.getInstance().updateReRunColFileLog(log);
	}

	/**
	 * 更新采集任务日志
	 * 
	 * @param lstInputRecord
	 * @return
	 */
	public static void updateColTaskLogReult(List<InputSourceRecordPO> lstInputRecord) {
		Set<Long> colLogIdLst = new HashSet<Long>();
		for (InputSourceRecordPO inputSourceRecordPO : lstInputRecord) {
			for (OutputSourceRecordPO out : inputSourceRecordPO.getLstOutputSourceRecord()) {
				colLogIdLst.add(out.getColLogId());
			}
		}

		for (Long colLogId : colLogIdLst) {
			updateColTaskLogSomeTimeReult(colLogId);
		}
	}

	/**
	 * 更新采集任务日志
	 * 
	 * @param lstInputRecord
	 * @return
	 */
	public static void updateColTaskLogSomeTimeReult(long colLogId) {
		// 初始化采集任务日志
		MRLog.systemOut("<col_log_id>" + colLogId + "</col_log_id>");
		// 查询采集任务的状态是否都成功
		MRColFileLog log = new MRColFileLog();
		log.setColLogid(colLogId);
		long count = MRLog.getInstance().getColFileLogStatus(log);
		if (-1 == count) {
			return;
		}
		log.setEndTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		log.setStatus(count > 0 ? MRColFileLog.STATUS_FAIL : MRColFileLog.STATUS_SUCCESS);
		MRLog.getInstance().updateColFileLog(log);
		printColResult(colLogId);
	}

	/**
	 * 设置输出列表的日志ID
	 * 
	 * @param colLogId
	 * @param lstInputRecord
	 */
	public static void setOutputSourceRecordColLogId(long colLogId, List<InputSourceRecordPO> lstInputRecord) {
		for (InputSourceRecordPO inputSourceRecordPO : lstInputRecord) {
			List<OutputSourceRecordPO> lstOut = inputSourceRecordPO.getLstOutputSourceRecord();
			for (OutputSourceRecordPO osrp : lstOut) {
				osrp.setColLogId(colLogId);
			}
		}
	}

	/**
	 * 获取采集任务结果
	 * 
	 * @param colLogId
	 * @return
	 */
	public static long[] queryColTaskLogResult(long colLogId) {
		if (colLogId != -1) {
			return MRLog.getInstance().queryColTaskLogResult(colLogId);
		}

		return new long[2];
	}

	/**
	 * 打印采集日志id
	 * 
	 * @param colLogId
	 */
	public static void printColLogId(Long colLogId) {
		MRLog.systemOut("<col_log_id>" + colLogId + "</col_log_id>");
	}

	/**
	 * 打印采集结果
	 * 
	 * @param colLogId
	 */
	public static void printColResult(Long colLogId) {
		long reault[] = SourceUtil.queryColTaskLogResult(colLogId);
		MRLog.systemOut("<col_success_count>" + reault[0] + "</col_success_count>");
		MRLog.systemOut("<col_fail_count>" + reault[1] + "</col_fail_count>");
	}

	/**
	 * 获取记录错误信息
	 * 
	 * @param dbValues
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static void redcordErrorMsg(String content, long colLogId) {
		if (null == content || content.trim().length() <= 0 || colLogId <= -1) {
			return;
		}

		List<MRColFileErrorLog> lst = null;
		try {
			lst = (List<MRColFileErrorLog>) SourceUtil.deserializable(content);
			for (MRColFileErrorLog mr : lst) {
				mr.setColLogId(colLogId);
				MRLog.getInstance().colFileErrorLog(mr);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 若RootPath最后是"/"开头，这去掉斜杠
	 * 
	 * @param rootPath
	 * @return
	 */
	private static String unitePath(String rootPath) {
		if (null == rootPath) {
			return rootPath;
		}

		if (rootPath.endsWith("/")) {
			return rootPath.substring(0, rootPath.length() - 1);
		}

		return rootPath;

	}

	public static void deleteLocalFile(String filePath) {
		if (null == filePath || filePath.trim().length() <= 0) {
			return;
		}
		File file = new File(filePath);
		if (file.exists() && file.isFile()) {
			MRLog.systemOut(MRLog.get6Space() + "成功清除目标文件:" + filePath);
			file.delete();
		}
	}

	/**
	 * 强制下载时要删除原来的输出文件
	 * 
	 * @param filePath
	 * @param fileSystem
	 * @param compress
	 */
	public static void deleteHDFSFile(String filePath, FileSystem fileSystem, int compress) {
		if (null == filePath || filePath.trim().length() <= 0 || null == fileSystem) {
			return;
		}
		try {
			if (compress == 1 && !filePath.endsWith(".gz")) {
				filePath += ".gz";
			}
			Path path = new Path(filePath);
			if (fileSystem.exists(path) && fileSystem.isFile(path)) {
				fileSystem.delete(path, true);
				MRLog.systemOut(MRLog.get6Space() + "成功清除目标文件:" + filePath);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	public static void deleteFTPFile(String filePath, IFileTransfer fileTransfer, String connectMax, String connectTime)
			throws FTPException, IOException {
		if (null == filePath || filePath.trim().length() <= 0 || null == fileTransfer) {
			return;
		}
		fileTransfer.delete(filePath, connectMax, connectTime);
		MRLog.systemOut(MRLog.get6Space() + "成功清除目标文件:" + filePath);
	}

	public static Map<FileAttributePO, String> getPathLocal(String inputPath, Pattern pattern, Set<String> lstRootPath) {
		Map<FileAttributePO, String> tempMap = new HashMap<FileAttributePO, String>();

		File file = new File(inputPath);
		if (file.exists() && file.isFile()) {
			if (StringUtil.matcher(file.getName(), pattern) && notContains(lstRootPath, file, true)) {
				FileAttributePO fap = new FileAttributePO();
				fap.setPath(file.getPath());
				fap.setLastModifyTime(file.lastModified());
				fap.setSize(file.length());
				tempMap.put(fap, inputPath);
			}
		} else if (file.exists() && file.isDirectory() && notContains(lstRootPath, file, true)) {
			getPathLocal(tempMap, file, inputPath, pattern, lstRootPath);
		}

		return tempMap;
	}

	private static boolean notContains(Set<String> lstRootPath, File file, boolean isSelf) {
		return notContains(lstRootPath, file.getPath(), isSelf);
	}

	private static boolean notContains(Set<String> lstRootPath, String path, boolean isSelf) {
		if (null == lstRootPath) {
			return true;
		}

		if (isSelf) {
			return true;
		}

		return !lstRootPath.contains(path);
	}

	private static void getPathLocal(Map<FileAttributePO, String> tempMap, File file, String srcPath, Pattern pattern,
			Set<String> lstRootPath) {
		File tmpLst[] = file.listFiles();
		for (File f : tmpLst) {
			if (f.exists() && f.isFile()) {
				if (StringUtil.matcher(f.getName(), pattern) && notContains(lstRootPath, f, false)) {
					FileAttributePO fap = new FileAttributePO();
					fap.setPath(f.getPath());
					fap.setLastModifyTime(f.lastModified());
					fap.setSize(f.length());
					tempMap.put(fap, srcPath);
				}
			} else if (f.exists() && f.isDirectory() && notContains(lstRootPath, f, false)) {
				getPathLocal(tempMap, f, srcPath, pattern, lstRootPath);
			}
		}
	}

	public static Map<FileAttributePO, String> getPathLocalHDFS(FileSystem fsystem, String[] inputPath,
			Pattern pattern, Set<String> lstRootPath) {
		try {
			return HDFSUtils.directoryList(fsystem, inputPath, pattern, lstRootPath);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static Map<FileAttributePO, String> getPathFTP(IFileTransfer ftp, String... inputPath) {
		Map<FileAttributePO, String> tempMap = new HashMap<FileAttributePO, String>();
		if (null == ftp || null == inputPath || inputPath.length <= 0) {
			return tempMap;
		}
		try {
			Map<FileAttribute, String> mapPath = ftp.directoryMap(inputPath);
			for (FileAttribute fa : mapPath.keySet()) {
				FileAttributePO fap = new FileAttributePO();
				fap.setPath(fa.getPath());
				fap.setSize(fa.getSize());
				fap.setLastModifyTime(fa.getLastModifyTime());
				tempMap.put(fap, mapPath.get(fa));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tempMap;
	}

	public static Map<FileAttributePO, String> getPathFTP(IFileTransfer ftp, String[] inputPath, Pattern pattern,
			Set<String> lstRootPath) {
		Map<FileAttributePO, String> tempMap = new HashMap<FileAttributePO, String>();
		if (null == ftp || null == inputPath || inputPath.length <= 0) {
			return tempMap;
		}
		try {
			Map<FileAttribute, String> mapPath = ftp.directoryMap(inputPath, pattern, lstRootPath);
			for (FileAttribute fa : mapPath.keySet()) {
				FileAttributePO fap = new FileAttributePO();
				fap.setPath(fa.getPath());
				fap.setSize(fa.getSize());
				fap.setLastModifyTime(fa.getLastModifyTime());
				tempMap.put(fap, mapPath.get(fa));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tempMap;
	}

	/**
	 * 获取相对于根路径的文件名称
	 * 
	 * @param inputPath
	 * @param inputRootPath
	 * @return
	 */
	public static String getFileName(String inputPath, String inputRootPath) {
		String fileName = "";
		if (!inputPath.equals(inputRootPath)) {
			int inputRootPathLen = inputRootPath.endsWith("/") ? inputRootPath.length() : inputRootPath.length() + 1;
			fileName = inputPath.substring(inputPath.indexOf(inputRootPath) + inputRootPathLen, inputPath.length());
		} else {
			fileName = inputPath.substring(inputPath.lastIndexOf("/") + 1);
		}

		return fileName;
	}

	/**
	 * 为输出文件进行更新赋值
	 * 
	 * @param osrp
	 */
	public static void setOsrp(OutputSourceRecordPO osrp) {
		long detailLogId = osrp.getColFileDetailId();
		MRColDetailLog mrColDetailLogTemp = MRLog.getInstance().getDetailLog(detailLogId);
		boolean moveOutStatus = (mrColDetailLogTemp.getMoveOutputStatus() == MRColDetailLog.MOVEOUTPUTSTATUS_FAIL);
		boolean reOutStatus = (mrColDetailLogTemp.getOutputRenameStatus() == MRColDetailLog.OUTPUTRENAMESTATUS_FAIL);
		boolean tem = (osrp.getStatus() == OutputSourceRecordPO.STATUS_SUCCESS) ||
				(mrColDetailLogTemp.getStatus() == MRColDetailLog.STATUS_SUCCESS);
		String outPath = mrColDetailLogTemp.getOutputPath();
		String moveOutPath = mrColDetailLogTemp.getMoveOutputPath();
		String outRename = mrColDetailLogTemp.getOutputRename();
		osrp.setStatus(tem ? OutputSourceRecordPO.STATUS_SUCCESS : osrp.getStatus());
		if (moveOutStatus) {
			osrp.setRootPath(outPath);
			osrp.setMovePath(moveOutPath);
		}
		if (reOutStatus) {
			osrp.setRename(outRename);
			osrp.setNewName(outRename);
			osrp.setRootPath(outPath);
		}

	}

	/**
	 * 为输入进行初始化
	 * 
	 * @param isrp
	 * @param mrColDetailLog
	 */
	public static void setIsrp(InputSourceRecordPO isrp, MRColDetailLog mrColDetailLog) {
		String inputPath = mrColDetailLog.getInputPath();
		String moveInputPath = mrColDetailLog.getMoveInputPath();
		String inputRename = mrColDetailLog.getInputRename();

		boolean moveInputStatus = (mrColDetailLog.getMoveInputStatus() == MRColDetailLog.MOVEINPUTSTATUS_FAIL);
		boolean deleteInputStatus = (mrColDetailLog.getDeleteInputStatus() == MRColDetailLog.DELETEINPUTSTATUS_FAIL);
		boolean reInputStatus = (mrColDetailLog.getRenameInputStatus() == MRColDetailLog.RENAMEINPUTSTATUS_FAIL);

		if (mrColDetailLog.getIsDoinputfiletype() == InputSourceRecordPO.dotype_move_rename) {
			if (!reInputStatus) {
				isrp.setNeedRename(false);
				isrp.setRootPath(inputPath);
				isrp.setMovePath(moveInputPath);
			} else {
				isrp.setMovePath(moveInputPath);
				isrp.setRename(inputRename);
				isrp.setRootPath(inputPath);
			}
		} else if (mrColDetailLog.getIsDoinputfiletype() == InputSourceRecordPO.dotype_del) {
			if (deleteInputStatus) {
				isrp.setNeedDelete(true);
			}
		} else if (mrColDetailLog.getIsDoinputfiletype() == InputSourceRecordPO.dotype_rename) {
			if (reInputStatus) {
				isrp.setRename(inputRename);
				isrp.setRootPath(inputPath);
			}
		} else if (mrColDetailLog.getIsDoinputfiletype() == InputSourceRecordPO.dotype_move) {
			if (moveInputStatus) {
				isrp.setMovePath(moveInputPath);
			}
		}
	}

	/**
	 * 获取文件名称
	 * 
	 * @param inputPath
	 * @param inputRootPath
	 * @return
	 */
	public static String getFileName(String path) {
		if (null == path || path.trim().length() <= 0) {
			return null;
		}
		return path.substring(path.lastIndexOf("/") + 1);
	}

	public static boolean isMatching(Pattern pattern, String value) {
		if (null == pattern) {
			return true;
		}

		if (null == value) {
			return false;
		}

		return pattern.matcher(value).matches();
	}

	public static void createParantPath(String targetPath) {
		if (null == targetPath || targetPath.trim().length() <= 0) {
			return;
		}

		File file = new File(targetPath);
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
	}

	/**
	 * @param inFileName
	 *            :SF_NAME
	 * @param renameRule
	 *            :((\d+)\.(\w+))
	 * @param rename
	 *            :sfile_$2_{SF_NAME}_{yyy}.data
	 */
	public static String getRenameResult(SourceRecordPO record) {
		String path = record.getPath();
		String renameRule = record.getRenameRule();
		String rename = record.getRename();

		if (null == path || path.trim().length() <= 0) {
			return null;
		}

		int index = path.lastIndexOf("/");
		String name = index == -1 ? path : path.substring(index + 1);
		if (null == renameRule || renameRule.trim().length() <= 0) {
			if (null == rename || rename.trim().length() <= 0) {
				return null;
			} else {
				return rename.replaceAll("\\{(?i)SF_NAME\\}", name);
			}
		} else {
			if (null == rename || rename.trim().length() <= 0) {
				return null;
			} else {
				String tempname = rename.replaceAll("\\{(?i)SF_NAME\\}", name);
				Pattern p = Pattern.compile(renameRule);
				Matcher m = p.matcher(name);
				String newName = tempname;
				int i = 0;
				while (m.find()) {
					String temp = m.group(0);
					newName = newName.replaceAll("\\$" + i, temp);
					i++;
				}

				return newName;
			}
		}
	}
}
