package com.ery.hadoop.mrddx.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCUtil {
	private Connection conn = null;

	public JDBCUtil(String driverName, String url, String user, String password) throws ClassNotFoundException,
			SQLException {
		Class.forName(driverName);
		conn = DriverManager.getConnection(url, user, password);
	}

	public JDBCUtil(String url, String user, String password) throws ClassNotFoundException, SQLException {
		String driverName = "";
		if (url != null && url.startsWith("jdbc:mysql")) {
			driverName = "com.mysql.jdbc.Driver";
		} else if (url != null && url.startsWith("jdbc:oracle")) {
			driverName = "oracle.jdbc.driver.OracleDriver";
		} else if (url != null && url.startsWith("jdbc:postgresql")) {
			driverName = "org.postgresql.Driver";
		}
		Class.forName(driverName);
		conn = DriverManager.getConnection(url, user, password);
	}

	/**
	 * 
	 * @param conn
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public ResultSet query(String sql) throws Exception {
		PreparedStatement psmt = null;
		if (conn != null) {
			try {
				psmt = conn.prepareStatement(sql);
				return psmt.executeQuery();
			} catch (Exception e) {
				throw new Exception(e);
			} finally {
				// this.close(psmt);
			}
		}
		return null;
	}

	/**
	 * 
	 * @param conn
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public PreparedStatement getPreparedStatement(Connection conn, String sql) throws Exception {
		PreparedStatement psmt = null;
		if (conn != null) {
			try {
				psmt = conn.prepareStatement(sql);
			} catch (Exception e) {
				throw new Exception(e);
			} finally {
				// this.close(psmt);
			}
		}
		return psmt;
	}

	/**
	 * @param conn
	 * @param psmt
	 * @throws Exception
	 */
	public void close(Connection conn, PreparedStatement psmt) {
		try {
			if (psmt != null)
				psmt.close();
		} catch (SQLException e) {
		}
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param conn
	 * @param psmt
	 * @throws Exception
	 */
	public void close(PreparedStatement psmt) {
		try {
			if (psmt != null)
				psmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @throws Exception
	 */
	public void close() {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}