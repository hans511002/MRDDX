/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ery.hadoop.mrddx.hbase;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Writable;

import com.ery.hadoop.mrddx.util.RuleConvert;

/**
 * Objects that are read from/written to a database should implement
 * <code>DBWritable</code>. DBWritable, is similar to {@link Writable} except
 * that the {@link #write(PreparedStatement)} method takes a
 * {@link PreparedStatement}, and {@link #readFields(ResultSet)} takes a
 * {@link ResultSet}.
 * <p>
 * Implementations are responsible for writing the fields of the object to
 * PreparedStatement, and reading the fields of the object from the ResultSet.
 * 
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



 * @createDate 2013-1-15
 * @version v1.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface HbaseWritable {
	/**
	 * 获取字段名称
	 * 
	 * @return 字段名称
	 */
	public String[] getFieldNames();

	/**
	 * 写入到hbase表
	 * 
	 * @param table
	 *            表名
	 * @param rowKeyRule
	 *            行规则
	 * @param rowRuleList
	 *            行规则列表
	 * @param clusterFieldNames
	 *            字段名
	 * @param splitSign 
	 * @param outColumnSplitRelations 
	 * @param outColumnDefaultValue 
	 * @throws IOException
	 *             IO异常
	 */
	public void write(HTable hTable, RuleConvert ruleConvertPO, String[][] clusterFieldNames, String[][] outColumnSplitRelations, String splitSign, HbaseRecordWriter<?, ?> hwriter) throws IOException;

	/**
	 * 删除hbase表数据
	 * 
	 * @param table
	 *            表名
	 * @param rowKeyRule
	 *            行规则
	 * @param rowRuleList
	 *            行规则列表
	 * @param fieldNames
	 *            字段名
	 * @throws IOException
	 *             IO异常
	 */
	public void delete(List<Delete> lstDel, RuleConvert ruleConvertPO, String[] fieldNames);

	
	/**
	 * 读取hbase表中的字段
	 * 
	 * @param KeyValue对象数组
	 * @throws IOException
	 */
	public void readFields(KeyValue[] keyValue) throws IOException;

	/**
	 * 读取hbase表中的字段
	 * 
	 * @param keyValue
	 *            KeyValue对象数组
	 * @param splitSign 拆分符号
	 * @param srcTargetFiledNameMap
	 *            格式(family:column1,target1,target2)
	 * @throws IOException
	 *             IO异常
	 */
	public void readFields(KeyValue[] keyValue, String splitSign, Map<String, String[]> srcTargetFiledNameMap) throws IOException;
}
