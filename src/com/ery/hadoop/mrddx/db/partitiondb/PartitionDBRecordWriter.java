package com.ery.hadoop.mrddx.db.partitiondb;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.ery.dimport.task.TaskClient;
import com.ery.dimport.task.TaskInfo;
import com.ery.dimport.task.TaskInfo.TaskType;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.FileMapper;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.partitiondb.route.PartitionByLong;
import com.ery.hadoop.mrddx.db.partitiondb.route.PartitionByString;
import com.ery.hadoop.mrddx.db.partitiondb.route.PartitionFunction;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

//接口表栅格+栅格获取日期
public class PartitionDBRecordWriter<K extends DBRecord, V> extends RecordWriter<K, V> {
	public final static ObjectMapper jsonMap = new ObjectMapper();

	FileMapper fileMap;
	protected Connection connection;
	long allRowCount = 0;
	long[] parRowCount = null;
	String tableName;

	public static final String COBAR_SPLIT_ISWRITETODB = "output.split.iswritetodb";
	public boolean isWriteToDb = true;
	public static final String COBAR_SPLIT_PARTITION_FIELDNAME = "output.split.partition.fieldname";
	public String partitionFiledName = "IMSI";
	public static final String COBAR_SPLIT_WRITE_FILES_PATH_DIR = "output.mr.mapred.file.target.Path";
	public String filePath = "/group/test/";// 目标文件名前缀
	public static final String COBAR_SPLIT_WRITE_FILES_SPLITFIELD_CHAR = "output.mr.mapred.file.field.split.chars";
	public String fieldSplitChar = ",";// 使用输入 文件的拆分符
	public static final String COBAR_SPLIT_WRITE_FILES_SPLITROW_CHAR = "output.mr.mapred.file.rows.split.chars";
	public String rowSplitChar = "\n";
	public static final String COBAR_SPLIT_WRITE_FILES_TO_MYSQL_DBNAME = "output.split.write.file.mysql.dbname";

	// 数据源参数,配置所有的主机,数据使用JSON 格式MAP{"主机名":"URL"}
	public static final String COBAR_SPLIT_PARTITION_MYSQLS_URL = "output.mr.mapred.jdbc.partition.urls";
	public static final String COBAR_SPLIT_PARTITION_IMPORT_DBTYPE = "output.mr.mapred.jdbc.partition.dbtype";
	public static final String COBAR_SPLIT_PARTITION_IMPORT_CMD = "output.mr.mapred.partition.import.cmd";
	public static final String COBAR_SPLIT_PARTITION_ISIMPORT_TODB = "output.mr.mapred.file.isimport.todb";
	public static final String COBAR_SPLIT_PARTITION_IMPORT_TODB_IN_EVERY_MAP = "output.mr.mapred.file.import.todb.in.everymap";
	public static final String COBAR_SPLIT_PARTITION_IMPORT_BASE_ZKNODE = "output.mr.mapred.partition.import.base.zknode";
	public Map<String, String> partitionUrlMap;

	public String[] partitionHostUrls = new String[] { "jdbc:mysql://wxdb02:3306/netdb",
			"jdbc:mysql://wxdb03:3306/netdb", "jdbc:mysql://wxdb04:3306/netdb", "jdbc:mysql://wxdb05:3306/netdb",
			"jdbc:mysql://wxdb07:3306/netdb", "jdbc:mysql://wxdb08:3306/netdb", "jdbc:mysql://wxdb09:3306/netdb",
			"jdbc:mysql://wxdb10:3306/netdb", "jdbc:mysql://wxdb11:3306/netdb", "jdbc:mysql://wxdb12:3306/netdb" };
	// 跟特定表相关
	// 各分区主机名称列表，按cobar的分区特定顺序
	public static final String COBAR_SPLIT_PARTITION_HOSTS = "output.split.partition.hosts";
	public String[] partitionHosts = new String[] { "wxdb02", "wxdb03", "wxdb04", "wxdb05", "wxdb07", "wxdb08",
			"wxdb09", "wxdb10", "wxdb11", "wxdb12" };
	public PartitionFunction partition = new PartitionByLong(new int[] { 4, 6 }, new int[] { 103, 102 });
	public static final String COBAR_SPLIT_PARTITION_FUNCTION_TYPE = "output.split.partition.function.type";
	String PartitionType = "int";// string
	public static final String COBAR_SPLIT_PARTITION_PARAMS_COUNT = "output.split.partition.params.count";
	int[] partitionCount;
	public static final String COBAR_SPLIT_PARTITION_PARAMS_LENGTH = "output.split.partition.params.length";
	int[] partitionLength;
	public static final String COBAR_SPLIT_PARTITION_PARAMS_HASHSLICE = "output.split.partition.params.hashslice";
	String PartitionHashSlice = "";// string分区有效

	public static java.text.DecimalFormat nft = new java.text.DecimalFormat("000");
	OutputStream[] partitionOut = null;
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public PartitionRecordWriter<K, V> DbWrite;
	public String destColumns;
	public String[] destColumnNames;
	WriteThread[] writeFileThread = null;
	public Exception ex = null;
	boolean isRunning = true;
	int beachSize = 3000;
	final TaskAttemptContext context;
	final Configuration conf;

	PartitionDBRecordWriter(Configuration conf) throws IOException {
		this.conf = conf;
		this.context = null;
		init(conf);
	}

	public PartitionDBRecordWriter(TaskAttemptContext context) throws IOException {
		this.context = context;
		this.conf = context.getConfiguration();
		init(conf);
	}

	public void init(Configuration conf) throws IOException {
		// 初始化文件输出流
		this.isWriteToDb = conf.getBoolean(COBAR_SPLIT_ISWRITETODB, true);
		DBConfiguration dbConf = new DBConfiguration(conf);
		partitionFiledName = conf.get(COBAR_SPLIT_PARTITION_FIELDNAME, "IMSI");
		// 数据源参数,配置所有的主机,数据使用JSON 格式MAP{"主机名":"URL"}
		String mysqlUrl = conf.get(COBAR_SPLIT_PARTITION_MYSQLS_URL, "{ \"wxdb02\":\"jdbc:mysql://wxdb02:3306/netdb\""
				+ ",\"wxdb03\":\"jdbc:mysql://wxdb03:3306/netdb\"" + ", \"wxdb04\":\"jdbc:mysql://wxdb04:3306/netdb\""
				+ ", \"wxdb05\":\"jdbc:mysql://wxdb05:3306/netdb\"" + ",\"wxdb07\":\"jdbc:mysql://wxdb07:3306/netdb\""
				+ ",\"wxdb08\": \"jdbc:mysql://wxdb08:3306/netdb\"" + ", \"wxdb09\":\"jdbc:mysql://wxdb09:3306/netdb\""
				+ ",\"wxdb10\":\"jdbc:mysql://wxdb10:3306/netdb\"" + ", \"wxdb11\":\"jdbc:mysql://wxdb11:3306/netdb\""
				+ ", \"wxdb12\":\"jdbc:mysql://wxdb12:3306/netdb\" }");
		partitionUrlMap = jsonMap.readValue(mysqlUrl, Map.class);

		partitionHosts = conf.getStrings(COBAR_SPLIT_PARTITION_HOSTS, new String[] { "wxdb02", "wxdb03", "wxdb04",
				"wxdb05", "wxdb07", "wxdb08", "wxdb09", "wxdb10", "wxdb11", "wxdb12" });
		partitionHostUrls = new String[partitionHosts.length];
		for (int i = 0; i < partitionHosts.length; i++) {
			partitionHostUrls[i] = partitionUrlMap.get(partitionHosts[i]);
			if (partitionHostUrls[i] == null) {
				throw new IOException("配置错误，数据源主机[" + partitionHosts[i] + "]在数据池" + partitionUrlMap + "中不存在");
			}
		}
		PartitionType = conf.get(COBAR_SPLIT_PARTITION_FUNCTION_TYPE, "int"); // string
		partitionCount = conf.getInts(COBAR_SPLIT_PARTITION_PARAMS_COUNT);
		partitionLength = conf.getInts(COBAR_SPLIT_PARTITION_PARAMS_LENGTH);
		if (PartitionType.equals("int")) {
			partition = new PartitionByLong(partitionCount, partitionLength);
		} else if (PartitionType.equals("string")) {
			PartitionHashSlice = conf.get(COBAR_SPLIT_PARTITION_PARAMS_HASHSLICE, "0");
			partition = new PartitionByString(partitionCount, partitionLength, PartitionHashSlice);
		} else {
			throw new IOException("配置错误，分库类型当前只支持[int,string]");
		}
		this.beachSize = conf.getInt(DBConfiguration.DB_OUTPUT_BUFFER_LENGTH, 3000);

		tableName = dbConf.getOutputTableName();
		this.destColumnNames = dbConf.getOutputFieldNames();
		this.destColumns = com.ery.base.support.utils.StringUtils.join(destColumnNames, ",");
		this.partitionFiledName = conf.get(COBAR_SPLIT_PARTITION_FIELDNAME, "IMSI");
		if (context == null)// 预处理检查使用
			DbWrite = new PartitionRecordWriter(conf, partition, partitionFiledName, partitionHostUrls, null);
		if (this.isWriteToDb) {
			String insertSql = PartitionRecordWriter.constructQuery(tableName, destColumnNames);
			System.out.println("Insert SQL:" + insertSql);// 日志
			if (context != null)
				DbWrite = new PartitionRecordWriter(context, partition, partitionFiledName, partitionHostUrls,
						insertSql);
		} else {
			filePath = conf.get(COBAR_SPLIT_WRITE_FILES_PATH_DIR, "/split/test/");// 目标文件目录
			if (!filePath.endsWith("/")) {
				filePath += "/";
			}
			fieldSplitChar = conf.get(COBAR_SPLIT_WRITE_FILES_SPLITFIELD_CHAR, ",");
			rowSplitChar = conf.get(COBAR_SPLIT_WRITE_FILES_SPLITROW_CHAR, "\n");
			System.out.println("fieldSplitChar=" + fieldSplitChar + "  rowSplitChar=" +
					rowSplitChar.replace("\n", "\\n"));

			int partitionNums = partition.getPartitionLength();
			partitionOut = new OutputStream[partitionNums];
			FileSystem fs = FileSystem.get(conf);
			Path p = new Path(filePath);
			if (!fs.exists(p))
				fs.mkdirs(p);
			if (context != null) {
				int atid = context.getTaskAttemptID().getTaskID().getId();
				parRowCount = new long[partitionHosts.length];
				for (int i = 0; i < partitionNums; i++) {
					String fileName = filePath + tableName + "." + nft.format(atid) + "." + partitionHosts[i];
					FSDataOutputStream out = fs.create(new Path(fileName), true);
					partitionOut[i] = out;
				}
				writeFileThread = new PartitionDBRecordWriter.WriteThread[partitionOut.length];
				for (int i = 0; i < partitionHosts.length; i++) {
					writeFileThread[i] = new WriteThread(this, i);
					writeFileThread[i].setDaemon(true);
					writeFileThread[i].setName("writeFileThread");
					if (this.context != null)
						writeFileThread[i].start();
				}
			}
		}
	}

	public String getDestFilePath() {
		return filePath;
	}

	public String getDestTableName() {
		return this.tableName;
	}

	public String[] getDestDataBase() {
		return conf.getStrings(COBAR_SPLIT_WRITE_FILES_TO_MYSQL_DBNAME);
	}

	public Map<String, String> getDestFiles() {
		if (context != null) {
			Map<String, String> map = new HashMap<String, String>();
			int atid = context.getTaskAttemptID().getTaskID().getId();
			for (int i = 0; i < partitionOut.length; i++) {
				String fileName = filePath + tableName + "." + nft.format(atid) + "." + partitionHosts[i];
				map.put(partitionHosts[i], fileName);
			}
			return map;
		} else {
			return null;
		}
	}

	public String[] getDbNames() {
		return conf.getStrings(COBAR_SPLIT_WRITE_FILES_TO_MYSQL_DBNAME);
	}

	public String getDbUser() {
		return conf.get(DBConfiguration.OUTPUT_USERNAME_PROPERTY, "");
	}

	public String getDbPass() {
		return conf.get(DBConfiguration.OUTPUT_PASSWORD_PROPERTY, "");
	}

	public String getFilePattern() {
		if (context != null) {
			int atid = context.getTaskAttemptID().getTaskID().getId();
			return tableName + "." + nft.format(atid) + ".{host}";
		} else {
			return tableName + ".\\d+.{host}";
		}
	}

	public String[] getDbHostUrls() {
		return partitionHostUrls;
	}

	public String[] getDbHosts() {
		return partitionHosts;
	}

	public String getDestColumns() {
		return destColumns;
	}

	public String getFieldSplitChar() {
		return fieldSplitChar;
	}

	public String getRowSplitChar() {
		return rowSplitChar;
	}

	// 主机及对应文件导入命令
	public String getImportCmd(String pathDir, int i) throws IOException {
		if (context == null) {
			throw new IOException("not in mr context");
		}
		int atid = context.getTaskAttemptID().getTaskID().getId();
		String[] dbNames = conf.getStrings(COBAR_SPLIT_WRITE_FILES_TO_MYSQL_DBNAME);
		String user = conf.get(DBConfiguration.OUTPUT_USERNAME_PROPERTY, "");
		String pwd = conf.get(DBConfiguration.OUTPUT_PASSWORD_PROPERTY, "");
		String fileName = pathDir + tableName + "." + nft.format(atid) + "." + partitionHosts[i];
		String importCmd = "mysqlimport --host=" + this.partitionHosts[i] + " --port=" +
				parseHostPort(this.partitionHostUrls[i]) + "  --user=" + user + " --password=" + pwd +
				"  --use-threads=2 " + " --replace --columns=" + destColumns + " --fields-terminated-by=" +
				fieldSplitChar + " --lines-terminated-by=" + this.rowSplitChar + " --fields-escaped-by=\\ " +
				dbNames[i] + " " + fileName;
		return importCmd;
	}

	public Map<String, String> getImportCmd(String pathDir) throws IOException {
		if (context == null) {
			throw new IOException("not in mr context");
		}
		Map<String, String> map = new HashMap<String, String>();
		int atid = context.getTaskAttemptID().getTaskID().getId();
		String[] dbNames = context.getConfiguration().getStrings(COBAR_SPLIT_WRITE_FILES_TO_MYSQL_DBNAME);
		String user = this.context.getConfiguration().get(DBConfiguration.OUTPUT_USERNAME_PROPERTY, "");
		String pwd = this.context.getConfiguration().get(DBConfiguration.OUTPUT_PASSWORD_PROPERTY, "");
		for (int i = 0; i < partitionOut.length; i++) {
			String fileName = pathDir + "/" + tableName + "." + nft.format(atid) + "." + partitionHosts[i];
			String importCmd = "mysqlimport --host=" + this.partitionHosts[i] + " --port=" +
					parseHostPort(this.partitionHostUrls[i]) + "  --user=" + user + " --password=" + pwd +
					"  --use-threads=2 " + " --replace --columns=" + destColumns + " --fields-terminated-by=" +
					this.fieldSplitChar + " --lines-terminated-by=" + this.rowSplitChar.replace("\n", "\\\\n") +
					" --fields-escaped-by=\\\\ " + dbNames[i] + " " + fileName;
			map.put(partitionHosts[i], importCmd);
		}
		return map;
	}

	// jdbc:mysql://wxdb12:3306/netdb62"
	static final java.util.regex.Pattern jdbcUrlPattern = Pattern.compile("jdbc:(\\w+)://(.+?):(\\d+).*");

	public static int parseHostPort(String url) {
		if (url != null) {
			Matcher m = jdbcUrlPattern.matcher(url);
			if (m.find()) {
				return Convert.toInt(m.group(3), 0);
			}
		}
		return 0;
	}

	private void exportImportOrder() throws IOException {
		if (this.isWriteToDb)
			return;
		if (context == null) {
			throw new IOException("not in mr context");
		}
		Configuration conf = context.getConfiguration();
		boolean imporToDb = conf.getBoolean(COBAR_SPLIT_PARTITION_ISIMPORT_TODB, true);
		if (!imporToDb)
			return;
		imporToDb = conf.getBoolean(COBAR_SPLIT_PARTITION_IMPORT_TODB_IN_EVERY_MAP, false);
		if (!imporToDb)
			return;
		context.setStatus("开始写文件导入命令");
		int DbType = Convert.toInt(conf.get(COBAR_SPLIT_PARTITION_IMPORT_DBTYPE), 1);
		List<TaskInfo> tasks = buildDimportTasks(this, DbType);
		try {
			String baseNode = conf.get(COBAR_SPLIT_PARTITION_IMPORT_BASE_ZKNODE, "/dimport");// 写dimport命令
			if (baseNode.equals("")) {
				baseNode = "/dimport";
			}
			TaskClient client = new TaskClient(conf.get(MRConfiguration.SYS_ZK_ADDRESS), baseNode);
			// MRZooKeeper zk = new
			// MRZooKeeper(conf.get(MRConfiguration.SYS_ZK_ADDRESS));
			for (TaskInfo task : tasks) {
				System.out.println("提交命令:" + task);
				client.submitTaskInfo(task);
				// zk.setData(orderNode, TaskInfo.Serialize(task));

			}
		} catch (IOException e) {
			System.err.println("写导入数据命令失败:" + tasks);
			e.printStackTrace(System.err);
		}
		context.setStatus("完成文件导入命令发布");
	}

	public void close(TaskAttemptContext context) throws IOException {
		System.out.println(sdf.format(new Date()) + " wrtie " + allRowCount + "rows");
		if (this.isWriteToDb) {
			if (DbWrite != null) {
				DbWrite.close(context);
			}
			DbWrite = null;
		} else {
			System.out.println("开始关闭处理线程");
			while (true) {
				boolean isAllEnd = true;
				for (WriteThread thread : writeFileThread) {
					synchronized (thread.metux) {
						thread.metux.notifyAll();
					}
					if (!thread.end)
						isAllEnd = false;
					if (ex != null) {
						throw new IOException(ex);
					}
				}
				if (isAllEnd)
					break;
				if (context != null) {
					context.setStatus("等待子处理线程结束  allRowCount:" + this.allRowCount);
				}
				System.out.println("等待子处理线程结束");
				Utils.sleep(500);
				isRunning = false;
			}
			if (partitionOut != null) {
				for (int i = 0; i < partitionOut.length; i++) {
					partitionOut[i].close();
					partitionOut[i] = null;
					context.getCounter("split output", "partition[" + i + "]").increment(parRowCount[i]);
					System.out.println("map partition[" + i + "]=" + parRowCount[i]);
				}
				this.context.getCounter("split output", "allRowCount").increment(allRowCount);
				// 些处不在处理，使用外部程序或者子类中进行入库
				int atid = context.getTaskAttemptID().getTaskID().getId();
				String[] dbNames = context.getConfiguration().getStrings(COBAR_SPLIT_WRITE_FILES_TO_MYSQL_DBNAME);
				String user = this.context.getConfiguration().get(DBConfiguration.OUTPUT_USERNAME_PROPERTY, "");
				String pwd = this.context.getConfiguration().get(DBConfiguration.OUTPUT_PASSWORD_PROPERTY, "");
				for (int i = 0; i < partitionOut.length; i++) {
					String fileName = filePath + tableName + "." + nft.format(atid) + "." + partitionHosts[i];
					String importCmd = "mysqlimport --host=" + this.partitionHosts[i] + " --port=" +
							parseHostPort(this.partitionHostUrls[i]) + "  --user=" + user + " --password=" + pwd +
							"  --use-threads=2 " + " --replace --columns=" + destColumns + " --fields-terminated-by=" +
							this.fieldSplitChar + " --lines-terminated-by=" + this.rowSplitChar.replace("\n", "\\\\n") +
							" --fields-escaped-by=\\\\ " + dbNames[i] + " " + fileName;
					System.out.println("外部导入命令，如果要在插件中关闭前执行,需要在插件中手动调用此关闭函数  importCmd=" + importCmd);
				}
				partitionOut = null;
			}
			context.setStatus("完成输出文件关闭");
			exportImportOrder();
		}
	}

	public static List<TaskInfo> buildDimportTasks(PartitionDBRecordWriter<?, ?> parWriter, int dbType) {
		List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		String[] dbNames = parWriter.getDbNames();
		String dbUser = parWriter.getDbUser();
		String dbPass = parWriter.getDbPass();
		String filePattern = parWriter.getFilePattern();
		String[] dbHosts = parWriter.getDbHosts();
		String destColumns = parWriter.getDestColumns();
		String fieldSplitChar = parWriter.getFieldSplitChar();
		String rowSplitChar = parWriter.getRowSplitChar();
		String destFilePath = parWriter.getDestFilePath();
		String destTableName = parWriter.getDestTableName();
		String[] dbHostUrls = parWriter.getDbHostUrls();
		Configuration conf = parWriter.conf;

		if (dbType == 1) {// mysql
			Map<String, List<String>> paramHosts = new HashMap<String, List<String>>();
			for (int i = 0; i < dbHosts.length; i++) {
				int port = PartitionDBRecordWriter.parseHostPort(dbHostUrls[i]);
				port = port > 0 ? port : 3306;
				String importCmd = "--host={host} --port=" + port + " --user=" + dbUser + " --password=" + dbPass +
						" --use-threads=3 --replace --columns=" + destColumns + " --fields-terminated-by=" +
						fieldSplitChar + " --lines-terminated-by=" + rowSplitChar.replace("\n", "\\n") +
						" --fields-escaped-by=\\ " + dbNames[i] + " {file}";
				List<String> hosts = paramHosts.get(importCmd);
				if (hosts == null) {
					hosts = new ArrayList<String>();
					paramHosts.put(importCmd, hosts);
				}
				hosts.add(dbHosts[i]);
			}
			String cmd = conf.get(COBAR_SPLIT_PARTITION_IMPORT_CMD, "mysql");
			if (cmd.trim().equals("")) {
				cmd = "mysql";
			}
			for (String params : paramHosts.keySet()) {
				TaskInfo task = new TaskInfo();
				task.TASK_NAME = conf.get(MRConfiguration.MAPRED_JOB_NAME) + "_" + destTableName;// 使用JOB名称
				task.cmd = cmd;
				task.taskType = TaskType.START;
				task.FILE_PATH = "hdfs://ns1" + destFilePath;
				task.FILE_FILTER = filePattern;
				task.params = new ArrayList<String>();
				task.params.add(params);
				if (paramHosts.size() > 1)
					task.hosts = paramHosts.get(params);
				tasks.add(task);
			}
		} else if (dbType == 2) {// infinidb 只能表所有 字段
			Map<String, List<String>> paramHosts = new HashMap<String, List<String>>();
			for (int i = 0; i < dbHosts.length; i++) {
				int port = PartitionDBRecordWriter.parseHostPort(dbHostUrls[i]);
				port = port > 0 ? port : 3306;
				String importCmd = " " + dbNames[i] + " " + destTableName + " {file}  -b 5000 -r 6 -w 12 -s " +
						fieldSplitChar + " ";
				List<String> hosts = paramHosts.get(importCmd);
				if (hosts == null) {
					hosts = new ArrayList<String>();
					paramHosts.put(importCmd, hosts);
				}
				hosts.add(dbHosts[i]);
			}
			String cmd = conf.get(COBAR_SPLIT_PARTITION_IMPORT_CMD, "sudo /usr/local/Calpont/bin/cpimport");
			if (cmd.trim().equals("")) {
				cmd = "sudo /usr/local/Calpont/bin/cpimport";
			}
			for (String params : paramHosts.keySet()) {
				TaskInfo task = new TaskInfo();
				task.TASK_NAME = conf.get(MRConfiguration.MAPRED_JOB_NAME) + "_" + destTableName;// 使用JOB名称
				task.cmd = cmd;
				task.taskType = TaskType.START;
				task.FILE_PATH = "hdfs://ns1" + destFilePath;
				task.FILE_FILTER = filePattern;
				task.params = new ArrayList<String>();
				task.params.add(params);
				if (paramHosts.size() > 1)
					task.hosts = paramHosts.get(params);
				tasks.add(task);
			}
		} else if (dbType == 3) {// oracle
			String cmd = conf.get(COBAR_SPLIT_PARTITION_IMPORT_CMD, "sqlldr");
			if (cmd.trim().equals("")) {
				cmd = "sqlldr";
			}
		}
		return tasks;
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		if (!isWriteToDb) {// 不入库，写文件
			// 直接判断记录分文件写入
			Map<String, Object> record = key.getRow();
			String imsi = record.get(partitionFiledName).toString();
			int partitionIndex = partition.calculate(imsi);
			if (imsi == null) {
				System.err.println("partition key is null row:" + record);
				this.context.getCounter("split output", "null partitionKey").increment(1);
				return;
			}
			writeFileThread[partitionIndex].add(record);
			parRowCount[partitionIndex]++;

			while (writeFileThread[partitionIndex].records.size() > beachSize * 3) {
				for (int i = 0; i < writeFileThread.length; i++) {
					synchronized (writeFileThread[i].metux) {
						writeFileThread[i].metux.notifyAll();
					}
				}
				synchronized (writeFileThread[partitionIndex].records) {
					writeFileThread[partitionIndex].records.wait(0);
				}
				// Utils.sleep(500);
			}
			// StringBuffer sb = new StringBuffer();
			// sb.append(record.get(destColumnNames[0]));
			// for (int i = 1; i < destColumnNames.length; i++) {
			// sb.append(fieldSplitChar);
			// sb.append(toString(record.get(destColumnNames[i])));
			// }
			// partitionOut[partitionIndex].write(sb.toString().getBytes());
			// partitionOut[partitionIndex].write(rowSplitChar.getBytes());
			// parRowCount[partitionIndex]++;
		} else {
			DbWrite.write(key, value);
		}
		allRowCount++;
		if (allRowCount % 10000 == 0)
			System.out.println(sdf.format(new Date()) + " wrtie " + allRowCount + "rows");
	}

	static java.text.NumberFormat dbf = new java.text.DecimalFormat(".######");

	public String toString(Object o) {
		if (o == null)
			return "";
		if (o instanceof Date) {
			return sdf.format((Date) o);
		}
		if (o instanceof Double || o instanceof Float) {
			return dbf.format(o);
		}
		return o.toString();
	}

	// 提交给子线程去提交数据，其它服务端的连接就不用等待
	public class WriteThread extends HasThread {
		public java.util.LinkedList<Map<String, Object>> records = new LinkedList<Map<String, Object>>();
		final PartitionDBRecordWriter write;
		final int partitionIndex;
		final Object metux = new Object();
		boolean end = true;

		public WriteThread(PartitionDBRecordWriter write, int index) throws IOException {
			this.write = write;
			this.partitionIndex = index;
		}

		public void close() throws IOException {
			System.out.println("关闭子处理线程:" + partitionIndex);
		}

		@Override
		public void run() {
			end = false;
			while (true) {
				try {
					synchronized (metux) {
						metux.wait(0);
					}
				} catch (InterruptedException e) {
				}
				try {
					Map<String, Object> map = null;
					synchronized (records) {
						if (!records.isEmpty())
							map = records.removeFirst();
						records.notifyAll();
					}
					while (map != null) {
						StringBuffer sb = new StringBuffer();
						sb.append(map.get(destColumnNames[0]));
						for (int i = 1; i < destColumnNames.length; i++) {
							sb.append(fieldSplitChar);
							sb.append(write.toString(map.get(destColumnNames[i])));
						}
						partitionOut[partitionIndex].write(sb.toString().getBytes());
						partitionOut[partitionIndex].write(rowSplitChar.getBytes());
						// dbWriter.write(new DBRecord(map), null);
						map = null;
						synchronized (records) {
							if (!records.isEmpty())
								map = records.removeFirst();
							records.notifyAll();
						}
					}
					if (!write.isRunning || write.ex != null) {
						close();
						end = true;
						break;
					}
					// Utils.sleep(500);
				} catch (Exception e) {
					synchronized (write) {
						write.ex = e;
						end = true;
						break;
					}
				} finally {
				}
			}
			end = true;
		}

		public void add(Map<String, Object> record) throws InterruptedException {
			synchronized (records) {
				records.add(record);
				if (records.size() > write.beachSize / 6) {
					synchronized (metux) {
						metux.notifyAll();
					}
				}
				records.notifyAll();
			}
		}
	}

	public static void processEnd(Configuration conf) throws IOException {
		boolean isWriteToDb = conf.getBoolean(PartitionDBRecordWriter.COBAR_SPLIT_ISWRITETODB, true);
		LogUtils.info("processend isWriteToDb=" + isWriteToDb);
		if (isWriteToDb) {
			return;
		}
		boolean imporToDb = conf.getBoolean(COBAR_SPLIT_PARTITION_ISIMPORT_TODB, true);
		LogUtils.info("processend isImportToDb=" + imporToDb);
		if (!imporToDb)
			return;
		imporToDb = conf.getBoolean(COBAR_SPLIT_PARTITION_IMPORT_TODB_IN_EVERY_MAP, false);
		LogUtils.info("processend isEveryMapToDb=" + imporToDb);
		if (imporToDb)
			return;
		int DbType = Convert.toInt(conf.get(COBAR_SPLIT_PARTITION_IMPORT_DBTYPE), 1);
		PartitionDBRecordWriter pw = new PartitionDBRecordWriter<>(conf);
		List<TaskInfo> tasks = buildDimportTasks(pw, DbType);
		try {
			String baseNode = conf.get(COBAR_SPLIT_PARTITION_IMPORT_BASE_ZKNODE, "/dimport");// 写dimport命令
			if (baseNode.equals("")) {
				baseNode = "/dimport";
			}
			TaskClient client = new TaskClient(conf.get(MRConfiguration.SYS_ZK_ADDRESS), baseNode);
			for (TaskInfo task : tasks) {
				System.out.println("提交命令:" + task);
				client.submitTaskInfo(task);
			}
		} catch (IOException e) {
			System.err.println("写导入数据命令失败:" + tasks);
			e.printStackTrace(System.err);
		}
		LogUtils.info("完成文件导入命令发布");
	}

}
