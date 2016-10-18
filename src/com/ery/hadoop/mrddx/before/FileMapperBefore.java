package com.ery.hadoop.mrddx.before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

import com.ery.hadoop.mrddx.DataTypeConstant;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * 在处理文件之前的预处理操作
 * 
 * @author wanghao
 * 
 */
public class FileMapperBefore implements IDBMRBefore {

	public String opType;
	public String fileId;
	public Connection conn;

	@Override
	public boolean before(Configuration conf) throws Exception {
		DBConfiguration dbconf = new DBConfiguration(conf);
		String tableName = dbconf.getOutputTableName();

		// 删除数据
		if (DataTypeConstant.MYSQLDATA.equals(this.opType) || DataTypeConstant.MYSQLROW.equals(this.opType) ||
				DataTypeConstant.ORACLEDATA.equals(this.opType) || DataTypeConstant.ORACLEROW.equals(this.opType) ||
				DataTypeConstant.RDBNORM.equals(this.opType) || DataTypeConstant.POSTGRESQL.equals(this.opType)) {

			// 获取链接
			try {
				this.conn = dbconf.getOutputConnection();
				String sql = "DELETE FROM " + tableName + " WHERE " + DBConfiguration.DB_OUTPUT_DATA_FILE_FILED + "=?";
				// 执行删除语句
				PreparedStatement statement = this.conn.prepareStatement(sql);
				statement.setString(0, this.fileId);
			} catch (SQLException e) {
				throw e;
			} finally {
				close();// 关闭连接
			}
		}

		return true;
	}

	@Override
	public boolean close() {
		if (null != this.conn) {
			try {
				this.conn.close();
			} catch (SQLException e) {
				MRLog.systemOut("mrbefore close conn error!");
			}
		}

		return false;
	}

	public String getOpType() {
		return opType;
	}

	public void setOpType(String opType) {
		this.opType = opType;
	}

	public String getFileId() {
		return fileId;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}
}
