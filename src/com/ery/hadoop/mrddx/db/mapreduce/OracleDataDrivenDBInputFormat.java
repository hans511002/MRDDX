package com.ery.hadoop.mrddx.db.mapreduce;

import java.io.IOException;
import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * 
 * Copyrights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 针对ORACLE的数据类型驱动拆分InputFormat类
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 * 
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OracleDataDrivenDBInputFormat<T extends DBWritable> extends DataDrivenDBInputFormat<T> implements
		Configurable {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(OracleDataDrivenDBInputFormat.class);

	@Override
	protected DBSplitter getSplitter(int sqlDataType) {
		switch (sqlDataType) {
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return new OracleDateSplitter();

		default:
			return super.getSplitter(sqlDataType);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Class<T> inputClass = (Class<T>) (this.dbConf.getInputClass());

		try {
			// Use Oracle-specific db reader
			return new OracleDataDrivenDBRecordReader<T>(split, inputClass, this.conf, getConnection(), this.dbConf,
					this.conditions, this.fieldNames, this.tableName);
		} catch (Exception e) {
			MRLog.errorException(LOG, "Create OracleDataDrivenDBRecordReader exception.", e);
			throw new IOException(StringUtil.stringifyException(e));
		}
	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setInputFormatClass(OracleDataDrivenDBInputFormat.class);
	}
}
