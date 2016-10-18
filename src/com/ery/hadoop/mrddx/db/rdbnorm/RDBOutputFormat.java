/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.ery.hadoop.mrddx.db.rdbnorm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBOutputFormat;
import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.base.support.utils.Convert;

/**
 * Output formatter for loading data to Vertica
 * 
 */
public class RDBOutputFormat<K extends DBWritable, V> extends DBOutputFormat<K, V> implements IHandleFormat {
	protected static final Log LOG = LogFactory.getLog(RDBOutputFormat.class);
	public static final String OUTPUT_TABLE_DATA_CLEARSQL = "mapred.rdb.output.table.clearsql";
	public static final String OUTPUT_TABLE_DATA_ALTERSQL = "mapred.rdb.output.table.altersql";
	public static final String OUTPUT_TABLE_DATA_INSERTSQL = "mapred.rdb.output.table.insertsql";

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
		if (conf.get(OUTPUT_TABLE_DATA_INSERTSQL, "").trim().equals("") &&
				conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, "").trim().equals("")) {
			throw new IOException("输出SQL和输出表不能同时为空");
		}
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = getConnection(conf);
			String clearSQL = conf.get(OUTPUT_TABLE_DATA_CLEARSQL, null);
			String alterSQL = conf.get(OUTPUT_TABLE_DATA_ALTERSQL, null);
			if (alterSQL != null && !alterSQL.trim().equals("")) {
				boolean cach = false;
				if (alterSQL.indexOf("$") > 0) {
					String tmp[] = alterSQL.split("$");
					cach = Convert.toBool(tmp[0], false);
					alterSQL = tmp[1];
				}
				try {
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
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
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

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setOutputFormatClass(RDBOutputFormat.class);
	}
}
