package com.ery.hadoop.mrddx.remote;

import java.io.Serializable;

public class OutputSourceRecordPO extends SourceRecordPO implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final int COMPRESS_NONE = 0;
	public static final int COMPRESS_GZ = 1;
	public static final int STATUS_SUCCESS = 0;
	public static final int STATUS_GET_SYSTEMSOURCE_FAIL = 1;
	public static final int STATUS_GET_OUTPUTSTREAM_FAIL = 2;
	private Object outputSourceSystem;
	protected long colFileDetailId; // 记录采集失败的日志id
	private boolean isDelFile = false; // 是否删除之前存在的文件
	private boolean isTranSuccess; // 用于传输成功
	private boolean isHandleOutputSuccess; // 用于表示详细日志输出处理成功标识符
	private long colLogId;
	private int Compress; // 是否进行压缩 0.否,1.GZ压缩

	public int getCompress() {
		return Compress;
	}

	public void setCompress(int compress) {
		Compress = compress;
	}

	public long getColLogId() {
		return colLogId;
	}

	public void setColLogId(long colLogId) {
		this.colLogId = colLogId;
	}

	public OutputSourceRecordPO() {
	}

	public long getColFileDetailId() {
		return colFileDetailId;
	}

	public void setColFileDetailId(long colFileDetailId) {
		this.colFileDetailId = colFileDetailId;
	}

	public Object getOutputSourceSystem() {
		return outputSourceSystem;
	}

	public void setOutputSourceSystem(Object outputSourceSystem) {
		this.outputSourceSystem = outputSourceSystem;
	}

	public boolean isDelFile() {
		return isDelFile;
	}

	public void setDelFile(boolean isDelFile) {
		this.isDelFile = isDelFile;
	}

	public boolean isTranSuccess() {
		return isTranSuccess;
	}

	public void setTranSuccess(boolean isTranSuccess) {
		this.isTranSuccess = isTranSuccess;
	}

	public boolean isMove() {
		String path = this.getMovePath();
		return null != path && path.trim().length() > 0 && isNeedMove;
	}

	public boolean isRename() {
		String name = this.getRename();
		return null != name && name.trim().length() > 0 && isNeedRename;
	}

	public boolean isHandleOutputSuccess() {
		return isHandleOutputSuccess;
	}

	public void setHandleOutputSuccess(boolean isHandleOutputSuccess) {
		this.isHandleOutputSuccess = isHandleOutputSuccess;
	}

	// @Override
	// public String getPath() {
	// if (null != path && path.endsWith(".tar")){
	// return path.substring(0, path.length()-4)+"_tar";
	// }
	//
	// if (null != path && path.endsWith(".tar.gz")){
	// return path.substring(0, path.length()-7)+"_tar.gz";
	// }
	// return path;
	// }

	@Override
	public String toString() {
		StringBuilder strb = new StringBuilder();
		strb.append("colId:" + colId);
		strb.append(";");
		strb.append("ouputDatasource:" + dataSource.toString());
		strb.append(";");
		strb.append("rootPath:" + rootPath);
		strb.append(";");
		strb.append("path:" + path);
		strb.append(";");
		strb.append("renameRule:" + renameRule);
		strb.append(";");
		strb.append("rename:" + rename);
		strb.append(";");
		strb.append("newName:" + newName);
		strb.append(";");
		strb.append("newPath:" + newPath);
		// strb.append(";");
		// strb.append("movePath:"+movePath);
		// strb.append(";");
		// strb.append("status:"+status);
		// strb.append(";");
		// strb.append("size"+size);
		return strb.toString();
	}

	/**
	 * 输出文件配置信息
	 * 
	 * @return
	 */
	public String getMetaInfo() {
		StringBuilder strb = new StringBuilder();
		strb.append("输出文件配置信息：");
		strb.append("\r\n");
		strb.append("    批次日志ID:" + colLogId + ", 详细日志ID:" + colFileDetailId);
		strb.append("\r\n");
		strb.append("    服务器地址：" + dataSource.getIp() + ":" + dataSource.getPort());
		strb.append("\r\n");
		strb.append("    用户名：" + dataSource.getUserName());
		strb.append("\r\n");
		strb.append("    配置根路径:" + rootPath);
		strb.append("\r\n");
		switch (Compress) {
		case 0:
			strb.append("    压缩类型:" + Compress + ", NONE");
			break;
		case 1:
			strb.append("    压缩类型:" + Compress + ", GZ");
			break;
		default:
			break;
		}
		strb.append("\r\n");
		strb.append("    文件路径:" + path);
		strb.append("\r\n");
		strb.append("    重命名规则:" + renameRule + ", 重命名表达式:" + rename);
		strb.append("\r\n");
		strb.append("    重命名后的文件名称:" + newName);
		strb.append("\r\n");
		strb.append("    移动目标目录:" + movePath);

		return strb.toString();
	}

	/**
	 * 输出文件配置信息
	 * 
	 * @return
	 */
	public String getFileInfo() {
		StringBuilder strb = new StringBuilder();
		strb.append("输出文件信息， 服务器地址：" + dataSource.getIp() + ":" + dataSource.getPort());
		strb.append(", 用户名：" + dataSource.getUserName());
		strb.append(", 文件路径:" + path);

		return strb.toString();
	}

	/**
	 * 输出文件配置信息
	 * 
	 * @return
	 */
	public String getFileResultInfo() {
		StringBuilder strb = new StringBuilder();
		strb.append("    输出文件信息：");
		strb.append("\r\n");
		strb.append("        服务器地址：" + dataSource.getIp() + ":" + dataSource.getPort());
		strb.append(", 用户名：" + dataSource.getUserName());
		strb.append(",输出的文件路径:" + path);
		strb.append(",移动后的文件目录:" + newPath);
		return strb.toString();
	}
}
