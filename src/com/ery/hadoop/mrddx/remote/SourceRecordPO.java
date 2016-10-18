package com.ery.hadoop.mrddx.remote;

import java.io.Serializable;

import com.ery.hadoop.mrddx.util.StringUtil;

public class SourceRecordPO implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final String rename_src_flag = "{SF_NAME}";
	protected int colId; // 采集任务ID号
	protected String note;// 备注
	protected FTPDataSourcePO dataSource; // 数据源
	protected String path; // 文件路径
	protected String rootPath;// 文件目录
	protected String renameRule;// 重命名规则(正则表达式)
	protected String rename; // 重命名新名称规则(模板名称)
	protected String newPath; // 重命名后对应的文件路径
	protected String newName; // 重命名后的文件名称
	protected String movePath;// 移动目录
	protected int status;// 状态
	protected long size;

	private boolean renameRuleStatus = false;// true:成功重命名文件, false:失败重命名文件
	private boolean movePathStatus = false;// true:成功移动文件，false:失败移动文件

	protected boolean isNeedDelete = false;
	protected boolean isNeedMoveRename = false;
	protected boolean isNeedMove = false;
	protected boolean isNeedRename = false;

	private boolean isFirst = true; // true:第一次执行标示符

	private int retrytimes; // 重传次数

	public SourceRecordPO() {
	}

	public int getColId() {
		return colId;
	}

	public void setColId(int colId) {
		this.colId = colId;
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getRootPath() {
		return rootPath;
	}

	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	public String getRenameRule() {
		return renameRule;
	}

	public void setRenameRule(String renameRule) {
		this.renameRule = renameRule;
		if (renameRule != null && !renameRule.equals("")) {
			this.setNeedRename(true);
		}
	}

	public String getRename() {
		return rename;
	}

	public void setRename(String rename) {
		this.rename = rename;
		if (rename != null && !rename.equals("")) {
			this.setNeedRename(true);
		}
	}

	public String getNewName() {
		return newName;
	}

	public void setNewName(String newName) {
		if (newName != null) {
			String nName = newName.replace(" ", "");
			if (nName != null)
				this.newName = nName;
			else
				this.newName = newName;
		} else {
			this.newName = newName;
		}
	}

	public String getRenameNewPath() {
		if (null == this.newName || this.newName.trim().length() <= 0) {
			return null;
		}

		String parent = StringUtil.getFilePathParentPath(path);
		return parent + "/" + this.newName;
	}

	public String getNewPath() {
		return newPath;
	}

	public void setNewPath(String newPath) {
		this.newPath = newPath;
	}

	public String getMovePath() {
		return movePath;
	}

	public void setMovePath(String movePath) {
		this.movePath = movePath;
		if (movePath != null && !movePath.equals(""))
			this.setNeedMove(true);
	}

	public String getMovePathFile() {
		if (null == this.movePath || this.movePath.trim().length() <= 0) {
			return null;
		}

		// 没有重命名，移动文件路径
		if (null == this.newName || this.newName.trim().length() <= 0) {
			String fileName = this.path.substring(this.path.lastIndexOf("/") + 1);
			return this.movePath.endsWith("/") ? (this.movePath + fileName) : (this.movePath + "/" + fileName);
		}

		// 有重命名，移动文件路径
		return this.movePath.endsWith("/") ? (this.movePath + this.newName) : (this.movePath + "/" + this.newName);
	}

	public FTPDataSourcePO getDataSource() {
		return dataSource;
	}

	public void setDataSource(FTPDataSourcePO dataSource) {
		this.dataSource = dataSource;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public boolean isRenameRuleStatus() {
		return renameRuleStatus;
	}

	public void setRenameRuleStatus(boolean renameRuleStatus) {
		this.renameRuleStatus = renameRuleStatus;
		if (isNeedMove) {
			movePathStatus = renameRuleStatus;
		}
	}

	public boolean isMovePathStatus() {
		return movePathStatus;
	}

	public void setMovePathStatus(boolean movePathStatus) {
		this.movePathStatus = movePathStatus;
		if (isNeedRename) {
			renameRuleStatus = movePathStatus;
		}
	}

	public String getSourceParentFlag() {
		return this.dataSource.getSourceSystemOnlyFlag() + ":" + this.rootPath;
	}

	public String getSourceOnlyFlag() {
		return this.dataSource.getSourceSystemOnlyFlag() + ":" + this.path;
	}

	public boolean isNeedDelete() {
		return isNeedDelete;
	}

	public void setNeedDelete(boolean isNeedDelete) {
		this.isNeedDelete = isNeedDelete;
	}

	public boolean isNeedMoveRename() {
		return isNeedMoveRename;
	}

	public void setNeedMoveRename(boolean isNeedMoveRename) {
		this.isNeedMoveRename = isNeedMoveRename;
		this.isNeedRename = true;
		this.isNeedMove = true;
	}

	public boolean isNeedMove() {
		return isNeedMove;
	}

	public void setNeedMove(boolean isNeedMove) {
		this.isNeedMove = isNeedMove;
		if (this.isNeedRename) {
			this.isNeedMoveRename = true;
		}
	}

	public boolean isNeedRename() {
		return isNeedRename;
	}

	public void setNeedRename(boolean isNeddRename) {
		this.isNeedRename = isNeddRename;
		if (this.isNeedMove) {
			this.isNeedMoveRename = true;
		}
	}

	public boolean isFirst() {
		return isFirst;
	}

	public void setFirst(boolean isFirst) {
		this.isFirst = isFirst;
	}

	public int getRetrytimes() {
		return retrytimes;
	}

	public void setRetrytimes(int retrytimes) {
		this.retrytimes = retrytimes;
	}
}
