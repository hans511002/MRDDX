package com.ery.hadoop.mrddx.db.mapreduce;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * Objects that are read from/written to a database should implement
 * <code>DBWritable</code>. DBWritable, is similar to {@link Writable} except
 * that the {@link #write(PreparedStatement)} method takes a
 * {@link PreparedStatement}, and {@link #readFields(ResultSet)} takes a
 * {@link ResultSet}.
 * <p>
 * Implementations are responsible for writing the fields of the object to
 * PreparedStatement, and reading the fields of the object from the ResultSet.
 * <p>
 * Example:
 * </p>
 * If we have the following table in the database :
 * 
 * <pre>
 * CREATE TABLE MyTable (
 *   counter        INTEGER NOT NULL,
 *   timestamp      BIGINT  NOT NULL,
 * );
 * </pre>
 * 
 * then we can read/write the tuples from/to the table with :
 * <p>
 * 
 * <pre>
 * public class MyWritable implements Writable, DBWritable {
 * 	// Some data
 * 	private int counter;
 * 	private long timestamp;
 * 
 * 	// Writable#write() implementation
 * 	public void write(DataOutput out) throws IOException {
 * 		out.writeInt(counter);
 * 		out.writeLong(timestamp);
 * 	}
 * 
 * 	// Writable#readFields() implementation
 * 	public void readFields(DataInput in) throws IOException {
 * 		counter = in.readInt();
 * 		timestamp = in.readLong();
 * 	}
 * 
 * 	public void write(PreparedStatement statement) throws SQLException {
 * 		statement.setInt(1, counter);
 * 		statement.setLong(2, timestamp);
 * 	}
 * 
 * 	public void readFields(ResultSet resultSet) throws SQLException {
 * 		counter = resultSet.getInt(1);
 * 		timestamp = resultSet.getLong(2);
 * 	}
 * }
 * </pre>
 * 
 * </p>
 * 



 * @createDate 2013-1-7
 * @version v1.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface DBWritable {
	/**
	 * 写入数据到数据库
	 * 
	 * @param statement
	 *            statement
	 * @param fieldNames
	 *            字段名称
	 * @throws SQLException
	 *             SQL异常
	 */
	public void write(PreparedStatement statement, String[] fieldNames) throws SQLException;

	/**
	 * 写入数据到数据库
	 * 
	 * @param statement
	 *            statement
	 * @throws SQLException
	 *             SQL异常
	 */
	public void write(PreparedStatement statement) throws SQLException;

	/**
	 * 从数据库读取数据
	 * 
	 * @param resultSet
	 *            结果集
	 * @param fieldNames
	 *            字段名
	 * @throws SQLException
	 *             SQL异常
	 */
	public void readFields(ResultSet resultSet, String[] fieldNames) throws SQLException;

	/**
	 * 从数据库读取数据
	 * 
	 * @param resultSet
	 *            结果集
	 * @throws SQLException
	 *             SQL异常
	 */
	public void readFields(ResultSet resultSet) throws SQLException;

	/**
	 * 获取字段名
	 * 
	 * @return 字段名列表
	 */
	public String[] getFieldNames();
}
