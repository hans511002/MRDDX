package com.ery.hadoop.mrddx.db.mapreduce;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBRecordReader;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * Copyrights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 运用数据类型驱动(Data-Driver)的拆分RecoordReader的实现类
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 * @param <T> key:LongWritables value:DBWritables
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PartitionDrivenDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(PartitionDrivenDBRecordReader.class);

	/**
	 * 构造方法
	 * 
	 * @param split 拆分对象
	 * @param inputClass 输入类型
	 * @param conf 配置对象
	 * @param conn 连接对象
	 * @param dbConfig 配置对象
	 * @param condition 条件
	 * @param fields 输入字段
	 * @param table 表名
	 * @param dbProduct 数据库产品名称
	 */
	public PartitionDrivenDBRecordReader(InputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
			DBConfiguration dbConfig, String condition, String[] fields, String table) {
		super(split, inputClass, conf, conn, dbConfig, condition, fields, table);
	}

	@Override
	protected String getSelectQuery() {
		PartitionDrivenDBInputFormat.PartitionDrivenDBInputSplit dataSplit = (PartitionDrivenDBInputFormat.PartitionDrivenDBInputSplit) getSplit();
		String inputQuery = this.dbConf.getInputQuery();
		String tableName = this.dbConf.getInputTableName();

		// Build the WHERE clauses associated with the data split first.
		// We need them in both branches of this function.
		if (inputQuery == null) {
			StringBuilder query = new StringBuilder();
			query.append("SELECT ");

			for (int i = 0; i < this.fieldNames.length; i++) {
				query.append(this.fieldNames[i]);
				if (i != this.fieldNames.length - 1) {
					query.append(", ");
				}
			}

			query.append(" FROM ").append(tableName).append(" partition(").append(dataSplit.partitionName).append(")");
			inputQuery = query.toString();
		} else {
			inputQuery = inputQuery.replaceAll("(?i)\\{SRC_TABLE\\}", dataSplit.partitionName);
		}

		MRLog.info(LOG, "Partition-Driver query: " + inputQuery);
		return inputQuery;
	}
}
