package com.ery.hadoop.mrddx.db.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.SequenceFile;
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
public interface FileWritable {
	/**
	 * 写入数据到文件
	 * 
	 * @param out
	 *            输出对象
	 * @param fieldNames
	 *            字段名称
	 * @throws IOException
	 *             IO异常
	 */
	public void write(DataOutput out, String[] fieldNames) throws IOException;

	/**
	 * 写入数据到文件
	 * 
	 * @param out
	 *            输出对象
	 * @param fieldNames
	 *            字段名称
	 * @param fieldSplitChars
	 *            列分隔符
	 * @throws IOException
	 *             IO异常
	 */
	public void write(DataOutput out, String[] fieldNames, byte[] fieldSplitChars) throws IOException;

	/**
	 * 写入数据到文件
	 * 
	 * @param out
	 *            输出对象
	 * @param fieldNames
	 *            字段名称
	 * @param fieldSplitChars
	 *            字段分隔符
	 * @param rowSplitChars
	 *            行分隔符
	 * @throws IOException
	 *             IO异常
	 */
	public void write(DataOutput out, String[] fieldNames, byte[] fieldSplitChars, byte[] rowSplitChars) throws IOException;
	
	/**
	 * 写入数据到文件
	 * 
	 * @param out
	 *            输出对象
	 * @param fieldNames
	 *            字段名称
	 * @param fieldSplitChars
	 *            字段分隔符
	 * @param rowSplitChars
	 *            行分隔符
	 * @throws IOException
	 *             IO异常
	 */
	public void write(DataOutput out, String[] fieldNames, byte[] fieldSplitChars, byte[] rowSplitChars, boolean isFisrtRow) throws IOException;
	
	/**
	 * 写入数据到文件(格式:Sequence)
	 * 
	 * @param out
	 *            输出对象
	 * @param fieldNames
	 *            目标字段
	 * @param fieldSplitChars
	 *            列分隔符
	 * @param rowSplitChars
	 *            行分隔符
	 * @throws IOException
	 *             IO异常
	 */
	public void write(SequenceFile.Writer out, String[] fieldNames, String fieldSplitChars, String rowSplitChars) throws IOException;

	/**
	 * 写入数据到文件(格式:RCFile)
	 * 
	 * @param out
	 *            输出对象
	 * @param fieldNames
	 *            目标字段
	 * @param fieldSplitChars
	 *            列分隔符
	 * @param rowSplitChars
	 *            行分隔符
	 * @throws IOException
	 *             IO异常
	 */
	public void write(RCFile.Writer out, String[] fieldNames, String fieldSplitChars, String rowSplitChars) throws IOException;

	/**
	 * 从文件中读取数据
	 * 
	 * @param in
	 *            输入对象
	 * @param fieldNames
	 *            字段名
	 * @throws IOException
	 *             IO异常
	 */
	public void readFields(DataInput in, String[] fieldNames) throws IOException;

	/**
	 * 从文件中读取数据
	 * 
	 * @param in
	 *            输入对象
	 * @param fieldNames
	 *            字段名
	 * @param fieldSplitChars
	 *            字段分隔符
	 * @throws IOException
	 */
	public void readFields(DataInput in, String[] fieldNames, String fieldSplitChars) throws IOException;

	/**
	 * 从文件中读取数据
	 * 
	 * @param in
	 *            输入对象
	 * @param fieldNames
	 *            字段名
	 * @param fieldSplitChars
	 *            字段分隔符
	 * @param rowSplitChars
	 *            行分隔符
	 * @throws IOException
	 *             IO异常
	 */
	public void readFields(DataInput in, String[] fieldNames, String fieldSplitChars, String rowSplitChars) throws IOException;
}
