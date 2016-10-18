package com.ery.hadoop.mrddx.remote;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class DBSourceRecordPO implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final int CONSTANT_INPUT_FILELST_TYPE_FTP = 0;// ftp
	public static final int CONSTANT_INPUT_FILELST_TYPE_DB = 1; // db

	private int colId; // 采集任务ID号
	private FTPDataSourcePO inputDatasource; // 输入数据源ID
	private int inputFilelstType; // 输入文件列表来源类型 ftp:0, db:1
	private Map<String, String> inputFilelstDatasourceParam; // 输入文件列表来源数据源ID
	private String inputQuerySql;// 输入文件列表查询SQL语句
	private String inputPath; // 文件输入根路径
	private String inputFileRule;// 输入文件规则(正则表达式)
	private Pattern inputFileRulePattern; // 输入文件规则
	private int inputDotype;// 输入文件的处理类型
	private String inputMovePath;// 输入文件移动目录
	private String inputRenameRule;// 输入文件重命名规则
	private String inputRename;// 输入文件重命名新名称
	private String note;// 备注
	private int compress; // 输出是否压缩 0,不压缩，1.GZ压缩
	private List<FileAttributePO> inputListFile; // 输入的文件列表

	private FTPDataSourcePO outputDatasource;// 输出数据源ID
	private String outputPath; // 文件输出路径
	private String outputRenameRule;// 输出文件重命名规则
	private String outputRename;// 输出文件重命名新名称

	public int getCompress() {
		return compress;
	}

	public void setCompress(int compress) {
		this.compress = compress;
	}

	private String outputMovePath;// 输出文件移动目录

	private int inputStatus = -1;// -1:正确，0:无法连接系统资源， 1:文件地址不存在
	private int outputStatus = -1;// -1:正确，0:无法连接系统资源， 1:目录无效，是文件
	private Object inputSourceSystem;
	private Object outputSourceSystem;

	public DBSourceRecordPO() {
	}

	public int getColId() {
		return colId;
	}

	public void setColId(int colId) {
		this.colId = colId;
	}

	public FTPDataSourcePO getInputDatasource() {
		return inputDatasource;
	}

	public void setInputDatasource(FTPDataSourcePO inputDatasource) {
		this.inputDatasource = inputDatasource;
	}

	public List<FileAttributePO> getInputListFile() {
		return inputListFile;
	}

	public void setInputListFile(List<FileAttributePO> inputListFile) {
		this.inputListFile = inputListFile;
	}

	public FTPDataSourcePO getOutputDatasource() {
		return outputDatasource;
	}

	public void setOutputDatasource(FTPDataSourcePO outputDatasource) {
		this.outputDatasource = outputDatasource;
	}

	public int getInputFilelstType() {
		return inputFilelstType;
	}

	public void setInputFilelstType(int inputDilelstType) {
		this.inputFilelstType = inputDilelstType;
	}

	public Map<String, String> getInputFilelstDatasourceParam() {
		return inputFilelstDatasourceParam;
	}

	public void setInputFilelstDatasourceParam(Map<String, String> inputFilelstDatasourceParam) {
		this.inputFilelstDatasourceParam = inputFilelstDatasourceParam;
	}

	public String getInputQuerySql() {
		return inputQuerySql;
	}

	public void setInputQuerySql(String inputQuerySql) {
		this.inputQuerySql = inputQuerySql;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getInputFileRule() {
		return inputFileRule;
	}

	public void setInputFileRule(String inputFileRule) {
		this.inputFileRule = inputFileRule;
		if (null == inputFileRule || inputFileRule.trim().length() <= 0) {
			return;
		}
		try {
			this.inputFileRulePattern = Pattern.compile(inputFileRule);
		} catch (PatternSyntaxException e) {
			if (inputFileRule.indexOf("{") >= 0) {
				System.out.println("输入文件过滤规则：" + inputFileRule + "  编译正则错误：" + e.getMessage());
			} else {
				throw e;
			}
		}
	}

	public Pattern getInputFileRulePattern() {
		return inputFileRulePattern;
	}

	public void setInputFileRulePattern(Pattern inputFileRulePattern) {
		this.inputFileRulePattern = inputFileRulePattern;
	}

	public int getInputDotype() {
		return inputDotype;
	}

	public void setInputDotype(int inputDotype) {
		this.inputDotype = inputDotype;
	}

	public String getInputMovePath() {
		return inputMovePath;
	}

	public void setInputMovePath(String inputMovePath) {
		this.inputMovePath = inputMovePath;
	}

	public String getInputRenameRule() {
		return inputRenameRule;
	}

	public void setInputRenameRule(String inputRenameRule) {
		this.inputRenameRule = inputRenameRule;
	}

	public String getInputRename() {
		return inputRename;
	}

	public void setInputRename(String inputRename) {
		this.inputRename = inputRename;
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getOutputRenameRule() {
		return outputRenameRule;
	}

	public void setOutputRenameRule(String outputRenameRule) {
		this.outputRenameRule = outputRenameRule;
	}

	public String getOutputRename() {
		return outputRename;
	}

	public void setOutputRename(String outputRename) {
		this.outputRename = outputRename;
	}

	public String getOutputMovePath() {
		return outputMovePath;
	}

	public void setOutputMovePath(String outputMovePath) {
		this.outputMovePath = outputMovePath;
	}

	public int getInputStatus() {
		return inputStatus;
	}

	public boolean isInputRight() {
		return inputStatus == -1;
	}

	public void setInputStatus(int inputStatus) {
		this.inputStatus = inputStatus;
	}

	public int getOutputStatus() {
		return outputStatus;
	}

	public boolean isOutputRight() {
		return outputStatus == -1;
	}

	public void setOutputStatus(int outputStatus) {
		this.outputStatus = outputStatus;
	}

	public Object getInputSourceSystem() {
		return inputSourceSystem;
	}

	public void setInputSourceSystem(Object inputSourceSystem) {
		this.inputSourceSystem = inputSourceSystem;
	}

	public Object getOutputSourceSystem() {
		return outputSourceSystem;
	}

	public void setOutputSourceSystem(Object outputSourceSystem) {
		this.outputSourceSystem = outputSourceSystem;
	}

	/**
	 * 输入唯一标示符
	 * 
	 * @return
	 */
	public String getInputUniqueKey() {
		StringBuilder strb = new StringBuilder();
		strb.append(this.inputDatasource.getSourceSystemOnlyFlag());
		switch (this.inputFilelstType) {
		case 0:
			strb.append(":");
			strb.append(this.inputPath);
			break;
		case 1:
			strb.append(":");
			strb.append(this.inputQuerySql);
			break;
		default:
			break;
		}

		return strb.toString();
	}

	/**
	 * 输出唯一标示符
	 * 
	 * @return
	 */
	public String getOutputUniqueKey() {
		StringBuilder strb = new StringBuilder();
		strb.append(this.outputDatasource.getSourceSystemOnlyFlag());
		strb.append(":");
		strb.append(this.outputPath);
		return strb.toString();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return toInputString() + "\r\n" + toOuputString();
	}

	public String toInputString() {
		StringBuilder strb = new StringBuilder();
		strb.append("输入:");
		strb.append("colId:" + colId);
		strb.append(";");
		strb.append("inputDatasource:" + inputDatasource.toString());
		strb.append(";");
		strb.append("inputFilelstType:" + inputFilelstType);
		strb.append(";");
		strb.append("inputFilelstDatasourceParam:" + inputFilelstDatasourceParam.toString());
		strb.append(";");
		strb.append("inputQuerySql:" + inputQuerySql);
		strb.append(";");
		strb.append("inputPath:" + inputPath);
		strb.append(";");
		strb.append("inputFileRule:" + inputFileRule);
		strb.append(";");
		strb.append("inputFileRulePattern:" + inputFileRulePattern);
		strb.append(";");
		strb.append("inputDotype:" + inputDotype);
		strb.append(";");
		strb.append("inputMovePath" + inputMovePath);
		strb.append(";");
		strb.append("inputRenameRule:" + inputRenameRule);
		strb.append(";");
		strb.append("inputRename:" + inputRename);
		strb.append(";");
		strb.append("inputListFile:" + (inputListFile == null ? "" : inputListFile.toString()));
		return strb.toString();
	}

	public String toOuputString() {
		StringBuilder strb = new StringBuilder();
		strb.append("输出:");
		strb.append("outputDatasource:" + outputDatasource.toString());
		strb.append(";");
		strb.append("outputPath:" + outputPath);
		strb.append(";");
		strb.append("outputRenameRule:" + outputRenameRule);
		strb.append(";");
		strb.append("outputRename:" + outputRename);
		strb.append(";");
		strb.append("outputMovePath:" + outputMovePath);
		return strb.toString();
	}
}
