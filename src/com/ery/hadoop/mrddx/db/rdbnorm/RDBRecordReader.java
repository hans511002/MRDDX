/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.ery.hadoop.mrddx.db.rdbnorm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.ery.base.support.log4j.LogUtils;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBInputFormat;
import com.ery.hadoop.mrddx.db.DBRecordReader;
import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.log.MRLog;

public class RDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

	public RDBRecordReader(InputSplit split, Class<T> inputClass, Configuration conf, Connection conn,
			DBConfiguration dbConfig, String conditions, String[] fields, String table) {
		super(split, inputClass, conf, conn, dbConfig, conditions, fields, table);
	}

	@Override
	protected ResultSet executeQuery(DBInputFormat.DBInputSplit split) throws SQLException {
		if (this.split instanceof RDBInputSplit) {
			RDBInputSplit vs = ((RDBInputSplit) this.split);
			MRLog.consoleDebug(LOG, "RDBInputSplit: " + vs.inputQuery);
			MRLog.consoleDebug(LOG, "              OFFSET= " + vs.getStart() + " length=" + vs.getLength() + "  ");
			MRLog.consoleDebug(LOG, "              params= " + vs.segmentParams);

			LogUtils.trace("Input split execute query");
			if (connection == null)
				throw new SQLException("Cannot execute query with no connection");
			String query = vs.inputQuery;
			long length = vs.getLength();
			if (length != 0) {
				query = "SELECT * FROM ( " + vs.inputQuery + " ) limited LIMIT " + length + " OFFSET " + vs.getStart();
			}
			LogUtils.debug("Query:" + query);
			statement = connection.prepareStatement(query);
			if (vs.segmentParams != null) {
				LogUtils.debug("Query:" + query + ". No. of params = " + vs.segmentParams.size());
				int i = 1;
				for (Object param : vs.segmentParams) {
					statement.setObject(i++, param);
					LogUtils.debug("With param :" + param.toString());
				}
			}
			LogUtils.debug("Executing query");
			ResultSet rs = statement.executeQuery();
			return rs;
		} else {
			throw new SQLException("inputsplit is not VerticaInputSplit");
		}
	}

	@Override
	protected String getSelectQuery() {
		if (this.split instanceof RDBInputSplit) {
			return ((RDBInputSplit) this.split).inputQuery;
		} else {
			return null;
		}
	}
}
