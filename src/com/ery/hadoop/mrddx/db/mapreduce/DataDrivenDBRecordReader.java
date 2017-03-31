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

 * 

 * @Comments 运用数据类型驱动(Data-Driver)的拆分RecoordReader的实现类

 * @version v1.0
 * @create Data 2013-1-9
 * @param <T> key:LongWritables value:DBWritables
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DataDrivenDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(DataDrivenDBRecordReader.class);

	// 数据库产品名称
	private String dbProductName;

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
	public DataDrivenDBRecordReader(InputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
			DBConfiguration dbConfig, String condition, String[] fields, String table, String dbProduct) {
		super(split, inputClass, conf, conn, dbConfig, condition, fields, table);
		this.dbProductName = dbProduct;
	}

	@Override
	protected String getSelectQuery() {
		StringBuilder query = new StringBuilder();
		DataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit = (DataDrivenDBInputFormat.DataDrivenDBInputSplit) getSplit();
		String inputQuery = this.dbConf.getInputQuery();

		// Build the WHERE clauses associated with the data split first.
		// We need them in both branches of this function.
		StringBuilder conditionClauses = new StringBuilder();
		conditionClauses.append("( ").append(dataSplit.getLowerClause());
		conditionClauses.append(" ) AND ( ").append(dataSplit.getUpperClause());
		conditionClauses.append(" )");
		if (inputQuery == null) {
			// We need to generate the entire query.
			query.append("SELECT ");

			for (int i = 0; i < this.fieldNames.length; i++) {
				query.append(this.fieldNames[i]);
				if (i != this.fieldNames.length - 1) {
					query.append(", ");
				}
			}

			query.append(" FROM ").append(this.tableName);
			if (!dbProductName.startsWith("ORACLE")) {
				// Seems to be necessary for hsqldb? Oracle explicitly does
				// *not*
				// use this clause.
				query.append(" AS ").append(this.tableName);
			}
			query.append(" WHERE ");
			if (this.conditions != null && this.conditions.length() > 0) {
				// Put the user's conditions first.
				query.append("( ").append(this.conditions).append(" ) AND ");
			}

			// Now append the conditions associated with our split.
			query.append(conditionClauses.toString());
		} else {
			// User provided the query. We replace the special token with our
			// WHERE clause.
			if (inputQuery.indexOf(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
				MRLog.warn(LOG, "在<" + DBConfiguration.INPUT_QUERY + ">不能找到查询条件的替换字符'"
						+ DataDrivenDBInputFormat.SUBSTITUTE_TOKEN + "': [" + inputQuery + "]. 并行拆分可能无法正常工作.");
			}

			query.append(inputQuery.replace(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN, conditionClauses.toString()));
		}

		MRLog.info(LOG, "Data-Driver query: " + query.toString());
		return query.toString();
	}
}
