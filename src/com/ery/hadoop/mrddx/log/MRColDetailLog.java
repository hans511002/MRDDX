package com.ery.hadoop.mrddx.log;

public class MRColDetailLog {
	public static final int STATUS_INIT=0;
	public static final int STATUS_SUCCESS=1;
	public static final int STATUS_FAIL= 2;
	
	public static final int ISOUTPUTRENAME_NO=0;// 不需要重命名输出文件
	public static final int ISOUTPUTRENAME_YES=1;// 需要重命名输出文件
	
	public static final int OUTPUTRENAMESTATUS_SUCCESS=0;// 成功重命名输出文件
	public static final int OUTPUTRENAMESTATUS_FAIL=1;// 失败重命名输出文件
	
	public static final int ISMOVEOUTPUT_NO=0;// 不需要移动
	public static final int ISMOVEOUTPUT_YES=1;// 需要移动
	
	public static final int MOVEOUTPUTSTATUS_SUCCESS=0;// 成功移动输出文件
	public static final int MOVEOUTPUTSTATUS_FAIL=1;// 失败移动输出文件
	
	public static final int DELETEINPUTSTATUS_SUCCESS=0;
	public static final int DELETEINPUTSTATUS_FAIL=1;
	
	public static final int MOVEINPUTSTATUS_SUCCESS=0;
	public static final int MOVEINPUTSTATUS_FAIL=1;
	
	public static final int RENAMEINPUTSTATUS_SUCCESS=0;
	public static final int RENAMEINPUTSTATUS_FAIL=1;
	
	public static final int ISDOINPUTFILETYPE_NO=0;//不处理
	public static final int ISDOINPUTFILETYPE_DELETE=1;//删除源文件
	public static final int ISDOINPUTFILETYPE_MOVE_RENAME=3;//移动源文件并重命名
	public static final int ISDOINPUTFILETYPE_MOVE=2;//移动源文件
	public static final int ISDOINPUTFILETYPE_RENAME=4;//重命名源文件
	
	
	
	private long id;
	private long fileId;
	private long colLogId;
	private int colId;
	private String startTime;
	private String endTime;
	private String inputFileName;
	private String outputFileName;
	private String inputPath; // 输入源文件的root目录
	private String outputPath; // 输入源文件的root目录
	private String inputRename; // 输入重命名名称
	private String outputRename; // 输出重命名名称
	private long fileSize;
	private int status;// 0:初始化,1:成功,2:失败
	private int isOutputRename;// 0:不需要重命名输出文件， 1：需要重命名输出文件
	private int outputRenameStatus; // 0:成功重命名输出文件, 1:失败重命名输出文件
	private int isMoveOutput;// 0:不需要移动,1:需要移动
	private String moveOutputPath;
	private int moveOutputStatus; // 0:成功移动输出文件，1：失败移动输出文件
	private int isDoinputfiletype;// 0:不处理，1:删除源文件，2:移动源文件到目标目录,3:移动源文件并重命名,4:重命名
	private int deleteInputStatus = -1;// 0:成功删除输入文件，1：失败删除输入文件
	private String moveInputPath;// 
	private int moveInputStatus;// 0:成功移动输入文件，1:失败移动输入文件
	private int renameInputStatus;// 0:成功重命名输入文件, 1:失败重命名输入文件

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getFileId() {
		return fileId;
	}

	public void setFileId(long fileId) {
		this.fileId = fileId;
	}

	public long getColLogId() {
		return colLogId;
	}

	public void setColLogId(long colLogId) {
		this.colLogId = colLogId;
	}

	public int getColId() {
		return colId;
	}

	public void setColId(int colId) {
		this.colId = colId;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getInputFileName() {
		return inputFileName;
	}

	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getInputRename() {
		return inputRename;
	}

	public void setInputRename(String inputRename) {
		this.inputRename = inputRename;
	}

	public String getOutputRename() {
		return outputRename;
	}

	public void setOutputRename(String outputRename) {
		this.outputRename = outputRename;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getIsOutputRename() {
		return isOutputRename;
	}

	public void setIsOutputRename(int isOutputRename) {
		this.isOutputRename = isOutputRename;
	}

	public int getOutputRenameStatus() {
		return outputRenameStatus;
	}

	public void setOutputRenameStatus(int outputRenameStatus) {
		this.outputRenameStatus = outputRenameStatus;
	}

	public int getIsMoveOutput() {
		return isMoveOutput;
	}

	public void setIsMoveOutput(int isMoveOutput) {
		this.isMoveOutput = isMoveOutput;
	}

	public String getMoveOutputPath() {
		return moveOutputPath;
	}

	public void setMoveOutputPath(String moveOutputPath) {
		this.moveOutputPath = moveOutputPath;
	}

	public int getMoveOutputStatus() {
		return moveOutputStatus;
	}

	public void setMoveOutputStatus(int moveOutputStatus) {
		this.moveOutputStatus = moveOutputStatus;
	}

	public int getIsDoinputfiletype() {
		return isDoinputfiletype;
	}

	public void setIsDoinputfiletype(int isDoinputfiletype) {
		this.isDoinputfiletype = isDoinputfiletype;
	}

	public int getDeleteInputStatus() {
		return deleteInputStatus;
	}

	public void setDeleteInputStatus(int deleteInputStatus) {
		this.deleteInputStatus = deleteInputStatus;
	}

	public String getMoveInputPath() {
		return moveInputPath;
	}

	public void setMoveInputPath(String moveInputPath) {
		this.moveInputPath = moveInputPath;
	}

	public int getMoveInputStatus() {
		return moveInputStatus;
	}

	public void setMoveInputStatus(int moveInputStatus) {
		this.moveInputStatus = moveInputStatus;
	}

	public int getRenameInputStatus() {
		return renameInputStatus;
	}

	public void setRenameInputStatus(int renameInputStatus) {
		this.renameInputStatus = renameInputStatus;
	}
}
