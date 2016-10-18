package com.ery.hadoop.mrddx.remote;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class InputSourceRecordPO extends SourceRecordPO  implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final int STATUS_SUCCESS = 0;
	public static final int STATUS_GET_SYSTEMSOURCE_FAIL = 1;
	public static final int STATUS_GET_INPUTSTREAM_FAIL = 2;
	
	public static final int dotype_none = 0;// 不处理
	public static final int dotype_del = 1; // 删除文件
	public static final int dotype_move = 2; // 移动文件
	public static final int dotype_move_rename = 3; // 移动文件并重命名
	public static final int dotype_rename = 4; //重命名
	
	private String fileRule;// 输入文件规则
	private int dotype;// 输入文件的处理类型
	private String lastModiyTime;// 文件最后修改时间
	private Object inputSourceSystem;
	
	private boolean deleteStatus;// true:成功删除输入文件，false：失败删除输入文件
	
	private List<OutputSourceRecordPO> lstOutputSourceRecord; // 输出列表
	
	private boolean isHandleInputSuccess; // 用于表示详细日志输入处理成功标识符

	public InputSourceRecordPO() {
		this.lstOutputSourceRecord = new ArrayList<OutputSourceRecordPO>();
	}

	public String getFileRule() {
		return fileRule;
	}

	public void setFileRule(String fileRule) {
		this.fileRule = fileRule;
	}

	public int getDotype() {
		return dotype;
	}

	public void setDotype(int dotype) {
		this.dotype = dotype;
	}

	public String getLastModiyTime() {
		return lastModiyTime;
	}

	public void setLastModiyTime(String lastModiyTime) {
		this.lastModiyTime = lastModiyTime;
	}

	public Object getInputSourceSystem() {
		return inputSourceSystem;
	}

	public void setInputSourceSystem(Object inputSourceSystem) {
		this.inputSourceSystem = inputSourceSystem;
	}
	
	public boolean isDeleteStatus() {
		return deleteStatus;
	}

	public void setDeleteStatus(boolean deleteStatus) {
		this.deleteStatus = deleteStatus;
	}

	public List<OutputSourceRecordPO> getLstOutputSourceRecord() {
		return lstOutputSourceRecord;
	}

	public void setLstOutputSourceRecord(List<OutputSourceRecordPO> lstOutputSourceRecord) {
		this.lstOutputSourceRecord = lstOutputSourceRecord;
	}

	public boolean isDelete(){
		return isNeedDelete && dotype == dotype_del;
	}
	
	public boolean isMoveRename(){
		return isNeedMoveRename && dotype == dotype_move_rename;
	}
	
	public boolean isMove(){
		return isNeedMove && dotype == dotype_move;
	}
	
	public boolean isRename(){
		return isNeedRename && dotype == dotype_rename;
	}

	public boolean isHandleInputSuccess() {
		return isHandleInputSuccess;
	}

	public void setHandleInputSuccess(boolean isHandleInputSuccess) {
		this.isHandleInputSuccess = isHandleInputSuccess;
	}

	@Override
	public String toString() {
		StringBuilder strb = new StringBuilder();
//		strb.append("colId:"+colId);
//		strb.append(";");
		strb.append("inputDatasource:"+dataSource.toString());
		strb.append(";");
		strb.append("rootPath:"+rootPath);
		strb.append(";");
		strb.append("path:"+path);
		strb.append(";");
		strb.append("renameRule:"+renameRule);
		strb.append(";");
		strb.append("rename:"+rename);
		strb.append(";");
//		strb.append("newName:"+newName);
//		strb.append(";");
//		strb.append("movePath:"+movePath);
//		strb.append(";");
//		strb.append("status:"+status);
//		strb.append(";");
//		strb.append("size"+size);
//		strb.append(";");
//		strb.append("fileRule:"+fileRule);
		strb.append(";");
		strb.append("dotype:"+dotype);
//		strb.append(";");
//		strb.append("lastModiyTime:"+lastModiyTime);
//		strb.append(";");
//		strb.append("sourceSystem:"+inputSourceSystem);
		return strb.toString();
	}
	
	/**
	 * 输入文件配置信息
	 * @return
	 */
	public String getMetaInfo(){
		StringBuilder strb = new StringBuilder();
		strb.append("输入文件配置信息：");
		strb.append("\r\n");
		strb.append("    服务器地址："+dataSource.getIp()+":"+dataSource.getPort());
		strb.append("\r\n");
		strb.append("    用户名："+dataSource.getUserName());
		strb.append("\r\n");
		strb.append("    配置根路径:"+rootPath);
		strb.append("\r\n");
		strb.append("    文件路径:"+path);
		strb.append("\r\n");
		strb.append("    重命名规则:"+renameRule+", 重命名表达式:"+rename);
		strb.append("\r\n");
		strb.append("    重命名后的文件名称:"+newName);
		strb.append("\r\n");
		strb.append("    移动目标目录:"+movePath);
		strb.append("\r\n");
		switch (dotype) {
		case dotype_none:
			strb.append("    文件处理类型:0 不处理");
			break;
		case dotype_del:
			strb.append("    文件处理类型:1 删除文件");
			break;		
		case dotype_move:
			strb.append("    文件处理类型:2 移动文件");
			break;
		case dotype_move_rename:
			strb.append("    文件处理类型:3 移动文件并重命名");
			break;
		case dotype_rename:
			strb.append("    文件处理类型:4 重命名");
			break;
		default:
			break;
		}
		
		return strb.toString();
	}
}
