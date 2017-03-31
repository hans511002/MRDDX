package com.ery.hadoop.mrddx.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.util.RuleConvert;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * A RecordWriter that writes the reduce output to a SQL table
 * 



 * @createDate 2013-1-15
 * @version v1.0
 * @param <K>
 * @param <V>
 */
@InterfaceStability.Evolving
public class HbaseRecordWriter<K extends HbaseWritable, V> extends RecordWriter<K, V> {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(HbaseRecordWriter.class);

	// HTable
	private HTable hTable;

	// rowkey规则
	private String rowKeyRule;

	// 输出字段(格式：family:column)
	private String[] fieldNames;

	// flush缓存的行数.
	private int bufferLength = 0;

	// 行的总数
	private int rowCount = 0;

	// rowkey规则转换.
	private RuleConvert ruleConvertPO;

	// 数据删除标示符
	private boolean dataDeleteFlag;

	// 数据删除的delete队列
	private List<Delete> lstDel;

	// 每次的开始时间
	// private long perStartTime;
	//
	// // 总的开始时间
	// private long startTime;

	// 设置当前写的缓存大小
	private long currentWriteBufferSize;

	// 设置写的缓存大小
	// private long writeBufferSize;

	// 字段拆分符号
	private String splitSign;

	// 列簇与列的对应关系
	String[][] clusterFieldNames = new String[0][0];

	// 输出字段,与clusterFieldNames索引一致
	String[][] outColumnSplitRelations = new String[0][0];

	// WAL日志类型
	private Durability durability;

	// rowkey的唯一值
	private boolean useOnlyFlag;

	// 任务ID的后4位
	private String taskId;

	// 作为rowkey唯一值的一部分(截取时间的后6位，然后递增)
	private long number;

	private HbaseConfiguration dbConf;

	/**
	 * 默认构造方法
	 */
	public HbaseRecordWriter() {
	}

	/**
	 * 构造方法
	 * 
	 * @param dbConf
	 *            配置信息
	 * @param hTable
	 *            HTable
	 */
	public HbaseRecordWriter(HbaseConfiguration dbConf, HTable hTable) {
		this.hTable = hTable;
		this.dbConf = dbConf;
		this.fieldNames = dbConf.getOutputHBaseFieldNames().split(",");
		this.bufferLength = dbConf.getOutputHBaseBufferLength();
		this.splitSign = dbConf.getOutputHBaseColumnSplitSign();
		List<String[]> list = new ArrayList<String[]>();// 获取列簇与列的对应关系
		List<String[]> rela = new ArrayList<String[]>();// 输出字段
		StringUtil.decodeOutColumnSplitRelation(dbConf.getOutputHBaseColumnRelation(), list, rela);
		this.clusterFieldNames = list.toArray(new String[0][0]);
		this.outColumnSplitRelations = rela.toArray(new String[0][0]);
		this.rowKeyRule = dbConf.getOutputHBaseRowKeyRule();
		this.ruleConvertPO = new RuleConvert(this.rowKeyRule);
		this.dataDeleteFlag = dbConf.getOutputHBaseDataDeleteFlag();
		// this.writeBufferSize = this.hTable.getWriteBufferSize();
		this.durability = dbConf.getOutputHBaseSetWalFlag();
		this.useOnlyFlag = dbConf.getOutputHBaseUseOnlyRowkey();
		if (this.dataDeleteFlag) {
			this.lstDel = new ArrayList<Delete>(this.bufferLength > 0 ? this.bufferLength : 10);
		}

		this.taskId = dbConf.getConf().get("mapred.tip.id"); // 获取任务id
		if (this.taskId != null && this.taskId.length() >= 4) {
			this.taskId = this.taskId.substring(this.taskId.length() - 4, this.taskId.length());
		}
		this.number = Long.parseLong(String.valueOf(System.currentTimeMillis()).substring(7, 13));
		// this.perStartTime = System.currentTimeMillis();
		// this.startTime = System.currentTimeMillis();
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (this.dataDeleteFlag) {
			if (this.lstDel.size() > 0) {
				this.hTable.delete(this.lstDel);
			}
			this.hTable.close();
			this.lstDel.clear();
		} else {
			this.hTable.close();
		}
	}

	@Override
	public void write(K key, V value) {
		if (key == null) {
			LOG.error("write key is null");
			// throw new IOException("write key is null");
		}

		if (this.hTable == null) {
			LOG.error("write key is null");
			// throw new IOException("write table is null");
		}

		String errorMsg = "";
		boolean isexception = false;
		while (true) {
			try {
				if (this.dataDeleteFlag) {
					// 删除并提交
					key.delete(this.lstDel, this.ruleConvertPO, this.fieldNames);
					this.rowCount++;
					if (this.bufferLength > 0 && this.rowCount % this.bufferLength == 0) {
						this.hTable.delete(this.lstDel);
						this.lstDel.clear();
					}
				} else {
					// 输出并提交
					if (this.useOnlyFlag) {
						number++;
					}
					key.write(this.hTable, this.ruleConvertPO, this.clusterFieldNames, this.outColumnSplitRelations,
							this.splitSign, this);
					this.rowCount++;
					// if (this.bufferLength > 0 && this.rowCount %
					// this.bufferLength == 0) {
					// MRLog.systemOut("htable flush len:" + this.bufferLength
					// +"  per time:"+(System.currentTimeMillis() -
					// this.perStartTime)
					// + "  total len:"+
					// this.rowCount+"  total time:"+(System.currentTimeMillis()
					// - this.startTime));
					// this.perStartTime = System.currentTimeMillis();
					// MRLog.systemOut("currentWriteBufferSize=" +
					// this.currentWriteBufferSize +", WriteBufferSize="+
					// this.writeBufferSize);
					// this.currentWriteBufferSize = 0;
					// this.hTable.flushCommits();// submit
					// }
				}

				// 打印日志
				if (this.rowCount % 200000 == 0) {
					LOG.info("insert " + this.rowCount + " rows");
				}

				break;
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error(e.getMessage());
				isexception = true;
				errorMsg = StringUtil.stringifyException(e);
			}

			if (isexception) {
				LOG.warn("write error Msg：" + errorMsg);
				this.getConnection();
			}
		}
	}

	private void getConnection() {
		int i = 0;
		while (true) {
			i++;
			LOG.warn("retry connect, times " + i);
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			try {
				dbConf.getConf().set("zookeeper.session.timeout", "180000");
				this.hTable = new HTable(dbConf.getConf(), dbConf.getOutputHBaseTableName());
				this.hTable.setWriteBufferSize(dbConf.getOutputHBaseWriteBuffersize());
				this.hTable.setAutoFlush(false, true);
				break;
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (i == 10) {
				LOG.error("Exceed the maximum times of reconnect");
			}
		}
	}

	/**
	 * 统计当前大小
	 */
	public void heapSize(Put put) {
		this.currentWriteBufferSize += put.heapSize();
	}

	/**
	 * 设置WAL日志标示
	 * 
	 * @param put
	 */
	public void setDurability(Put put) {
		if (this.durability != null) {
			put.setDurability(this.durability);
		}
	}

	public boolean isUseOnlyFlag() {
		return useOnlyFlag;
	}

	public String getOnlyRowkey() {
		return this.taskId + this.number;
	}

}
