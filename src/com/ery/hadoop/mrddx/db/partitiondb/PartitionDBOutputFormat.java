/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.ery.hadoop.mrddx.db.partitiondb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.IMROutputFormatEnd;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBOutputFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.base.support.utils.Convert;

/**
 * Output formatter for loading data to Vertica
 * 
 */
public class PartitionDBOutputFormat<K extends DBRecord, V> extends DBOutputFormat<K, V> implements IHandleFormat,
		IMROutputFormatEnd {
	protected static final Log LOG = LogFactory.getLog(PartitionDBOutputFormat.class);
	public static final String OUTPUT_TABLE_DATA_CLEARSQL = "mapred.rdb.output.table.clearsql";
	public static final String OUTPUT_TABLE_DATA_ALTERSQL = "mapred.rdb.output.table.altersql";
	public static final String OUTPUT_TABLE_DATA_INSERTSQL = "mapred.rdb.output.table.insertsql";

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		System.out.println("TaskAttemptContext className:" + context.getClass().getCanonicalName());
		return new PartitionDBRecordWriter<K, V>(context);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws InterruptedException
	 */
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		super.checkOutputSpecs(context);
		checkOutputSpecs(context.getConfiguration());
	}

	public void checkOutputSpecs(Configuration conf) throws IOException {
		String clearSQL = conf.get(OUTPUT_TABLE_DATA_CLEARSQL, null);
		String alterSQL = conf.get(OUTPUT_TABLE_DATA_ALTERSQL, null);
		if (alterSQL == null && clearSQL == null) {
			return;
		}
		System.out.println("参数配置检查及预处理");
		// 需要每台机器执行
		PartitionDBRecordWriter<DBRecord, Object> pwriter = new PartitionDBRecordWriter<>(conf);
		try {
			for (int i = 0; i < pwriter.partitionHosts.length; i++) {
				Statement stmt = null;
				Connection conn = pwriter.DbWrite.runThread[i].connection;
				if (alterSQL != null && !alterSQL.trim().equals("")) {
					boolean cach = false;
					if (alterSQL.indexOf("$") > 0) {
						String tmp[] = alterSQL.split("$");
						cach = Convert.toBool(tmp[0], false);
						alterSQL = tmp[1];
					}
					try {
						System.out.println(pwriter.partitionHosts[i] + " 执行SQL：" + alterSQL);
						stmt = conn.createStatement();
						stmt.execute(alterSQL);
					} catch (Exception e) {
						if (!cach)
							throw new IOException(e);
					} finally {
						stmt.close();
					}
				}
				if (clearSQL != null && !clearSQL.trim().equals("")) {
					boolean cach = false;
					if (clearSQL.indexOf("$") > 0) {
						String tmp[] = clearSQL.split("$");
						cach = Convert.toBool(tmp[0], false);
						clearSQL = tmp[1];
					}
					try {
						System.out.println(pwriter.partitionHosts[i] + " 执行SQL：" + clearSQL);
						stmt = conn.createStatement();
						stmt.execute(clearSQL);
					} catch (Exception e) {
						if (!cach)
							throw new IOException(e);
					} finally {
						stmt.close();
						stmt = null;
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			System.out.println("完成检查和预处理");
			pwriter.DbWrite.close(null);
		}
	}

	/** (@inheritDoc) */
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

	public String constructQuery(String table, String[] fieldNames) {
		String insertSql = this.conf.get(OUTPUT_TABLE_DATA_INSERTSQL, "");
		if (insertSql != null && !insertSql.trim().equals("")) {
			return insertSql;
		}
		return super.constructQuery(table, fieldNames);
	}

	public boolean isNeedCheckParams(String parName) {
		return !parName.equals(DBConfiguration.OUTPUT_URL_PROPERTY);
	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setOutputFormatClass(PartitionDBOutputFormat.class);
		// 数据库连接
		String url = conf.getConfiguration().get(PartitionDBRecordWriter.COBAR_SPLIT_PARTITION_MYSQLS_URL, null);
		if (null == url || url.length() <= 0) {
			String meg = "数据库地址<" + PartitionDBRecordWriter.COBAR_SPLIT_PARTITION_MYSQLS_URL + ">未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
	}

	@Override
	public void processEnd(Configuration conf) throws Exception {
		PartitionDBRecordWriter.processEnd(conf);
	}
}
