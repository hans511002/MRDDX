package com.ery.hadoop.mrddx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ery.hadoop.mrddx.db.mapreduce.DBWritable;
import com.ery.hadoop.mrddx.db.mapreduce.FileWritable;
import com.ery.hadoop.mrddx.hbase.HbaseConfiguration;
import com.ery.hadoop.mrddx.hbase.HbaseRecordWriter;
import com.ery.hadoop.mrddx.hbase.HbaseWritable;
import com.ery.hadoop.mrddx.util.RuleConvert;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * Mapper,Reducer输出格式 Copyrights @ 2012,Tianyuan DIC Information Co.,Ltd. All
 * rights reserved.
 * 
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 */
public class DBRecord implements HbaseWritable, FileWritable, DBWritable, Writable, WritableComparable<Object>,
		RawComparator<DBRecord> {
	/**
	 * 行数据对应列的名称和值
	 */
	private Map<String, Object> row = new HashMap<String, Object>(300);

	/**
	 * 记录是否有效(true:有效，false：无效)
	 */
	private boolean status = true;

	/**
	 * 构造方法
	 */
	public DBRecord() {

	}

	/**
	 * 构造方法
	 * 
	 * @param row
	 *            记录
	 */
	public DBRecord(Map<String, Object> row) {
		this.row = row;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	/**
	 * 构造方法
	 * 
	 * @param names
	 *            字段名称
	 * @param values
	 *            字段值
	 */
	public DBRecord(String[] names, Object[] values) {
		this.row.clear();
		for (int i = 0; i < names.length; i++) {
			if (values.length > i) {
				this.row.put(names[i], values[i]);
			} else {
				this.row.put(names[i], null);
			}
		}
	}

	/**
	 * 获取Mapper输出
	 * 
	 * @param destFields
	 *            目标字段名称
	 * @param srcFields
	 *            源字段名称
	 * @throws IOException
	 *             IO异常
	 */
	public void addChange(DBRecord record, String[] destFields, String[] srcFields) throws IOException {
		for (int i = 0; i < destFields.length; i++) {
			// 存放输出字段数据
			Object o = record.row.get(srcFields[i]);
			if (o == null || o.equals("")) {
				if (this.row.get(destFields[i]) == null)
					this.putData(destFields[i], o);
			} else {
				this.putData(destFields[i], o);
			}

		}
	}

	/** 兼容老程序 */
	public DBRecord change(String[] destFields, String[] srcFields) throws IOException {
		DBRecord record = new DBRecord();
		for (int i = 0; i < destFields.length; i++) {
			// 存放输出字段数据
			record.putData(destFields[i], this.row.get(srcFields[i]));
		}
		return record;
	}

	/**
	 * 获取Mapper的输出
	 * 
	 * @param destFields
	 *            目标字段名称
	 * @param srcFields
	 *            源字段名称
	 * @param groupFieldMethods
	 *            方法
	 * @throws IOException
	 *             IO异常
	 */
	public DBRecord[] splitGroup(String[] destFields, String[] srcFields, String[] groupFieldMethods)
			throws IOException {
		DBRecord[] gps = new DBRecord[2];
		gps[0] = new DBRecord(); // 分组的DBRecord
		gps[1] = new DBRecord();
		for (int i = 0; i < destFields.length; i++) {
			Integer method = DBGroupReducer.GROUP_METHODS.get(groupFieldMethods[i]);
			if (method == null) {
				throw new IOException("未知道的字段分组计算方法:" + groupFieldMethods[i]);
			}

			if (method == 0) {
				// 存放输出分组字段数据.
				gps[0].putData(destFields[i], this.row.get(srcFields[i]));
			} else {
				// 存放输出统计字段数据
				gps[1].putData(destFields[i], this.row.get(srcFields[i]));
			}
		}

		return gps;
	}

	/**
	 * 获取Mapper的输出
	 * 
	 * @param destFields
	 *            目标字段名称
	 * @param srcFields
	 *            源字段名称
	 * @param groupFieldMethods
	 *            方法
	 * @throws IOException
	 *             IO异常
	 */
	public DBRecord[] splitPartition(String[] destFields, String[] srcFields, String[] partitionFields)
			throws IOException {
		DBRecord[] gps = new DBRecord[2];
		if (partitionFields == null || partitionFields.length <= 0) {
			return null;
		}
		gps[0] = new DBRecord(); // 分组的DBRecord
		gps[1] = new DBRecord();
		for (int i = 0; i < destFields.length; i++) {
			String destField = destFields[i];
			for (int j = 0; j < partitionFields.length; j++) {
				String partion = partitionFields[j];
				if (partion == null || partion.trim().length() <= 0) {
					throw new IOException("未知道的分区字段:" + partitionFields[j]);
				}
				if (partion.equals(destField)) {
					// 存放输出分组字段数据.
					gps[0].putData(destFields[i], this.row.get(srcFields[i]));
				}
				// 存放输出统计字段数据
				gps[1].putData(destFields[i], this.row.get(srcFields[i]));
			}

		}

		return gps;
	}

	/**
	 * 获取GroupReduce的输出
	 * 
	 * @param destFields
	 *            目标字段名称
	 * @param srcFields
	 *            源字段名称
	 * @param groupFieldMethods
	 *            方法
	 * @throws IOException
	 *             IO异常
	 */
	public DBRecord[] splitPartition(String[] destFields, DBRecord sum, String[] partitionFields) throws IOException {
		DBRecord[] gps = new DBRecord[2];
		gps[0] = new DBRecord(); // 分组的DBRecord
		gps[1] = new DBRecord();
		this.mergerRecord(sum);
		for (int i = 0; i < destFields.length; i++) {
			String destField = destFields[i];
			for (int j = 0; j < partitionFields.length; j++) {
				String partion = partitionFields[j];
				if (partion == null || partion.trim().length() <= 0) {
					throw new IOException("未知道的分区字段:" + partitionFields[j]);
				}
				if (partion.equals(destField)) {
					// 存放输出分区字段数据.
					gps[0].putData(destFields[i], this.row.get(destFields[i]));
				}

				// 存放输出统计字段数据
				gps[1].putData(destFields[i], this.row.get(destFields[i]));
			}

		}

		return gps;
	}

	/**
	 * 与当前记录合并
	 * 
	 * @param val
	 *            被合并的记录
	 * @return 合并后的记录
	 */
	public DBRecord mergerRecord(DBRecord val) {
		for (String name : val.getRow().keySet()) {
			if (val.getData(name) != null)
				this.row.put(name, val.getData(name));
		}

		return this;
	}

	/**
	 * 添加数据
	 * 
	 * @param key
	 *            字段名
	 * @param value
	 *            字段值
	 */
	public void putData(String key, Object value) {
		this.row.put(key, value);
	}

	/**
	 * 根据字段名称值
	 * 
	 * @param key
	 *            字段名称
	 * @return 值
	 */
	public Object getData(String key) {
		return this.row.get(key);
	}

	/**
	 * 判断是否包含字段
	 * 
	 * @param key
	 *            字段名称
	 * @return true表示包含，否则不包含
	 */
	public boolean containsKeyData(String key) {
		return this.row.containsKey(key);
	}

	/**
	 * 移除字段
	 * 
	 * @param key
	 *            字段名称
	 * @return 被移除字段的值
	 */
	public Object removeData(String key) {
		return this.row.remove(key);
	}

	@Override
	public String[] getFieldNames() {
		return this.row.keySet().toArray(new String[0]);
	}

	/**
	 * 清空记录
	 */
	public void clear() {
		this.row.clear();
	}

	/**
	 * 获取当前记录
	 * 
	 * @return 记录
	 */
	public Map<String, Object> getRow() {
		return row;
	}

	@Override
	public boolean equals(Object o) {
		return toString().equals(o.toString());
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (String key : this.row.keySet()) {
			buf.append(key);
			buf.append(":");
			buf.append(this.row.get(key) + ",");
		}

		if (buf.length() > 0) {
			buf.deleteCharAt(buf.length() - 1);
		}
		return buf.toString();
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public int compareTo(Object o) {
		return this.toString().compareTo(o.toString());
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		this.write(statement, null);
	}

	@Override
	public void write(PreparedStatement statement, String[] fieldNames) throws SQLException {
		if (fieldNames == null) {
			int i = 0;
			for (String name : this.row.keySet()) {
				statement.setObject(i + 1, this.row.get(name));
			}
		} else {
			for (int i = 0; i < fieldNames.length; i++) {
				statement.setObject(i + 1, this.row.get(fieldNames[i]));
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringBuilder strBuilder = new StringBuilder();
		for (String name : this.row.keySet()) {
			strBuilder.append(name);
			strBuilder.append(":");
			Object obj = this.row.get(name);
			if (null != obj) {
				strBuilder.append(obj.toString());
			}
			strBuilder.append(",");
		}

		String tmp = strBuilder.toString();
		if (tmp.length() == 0) {
			out.writeInt(0);
			return;
		}

		tmp = tmp.substring(0, strBuilder.length() - 1);
		byte[] byteValue = tmp.getBytes();
		out.writeInt(byteValue.length);
		out.write(byteValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.row.clear();
		byte bt[] = null;
		try {
			int len = in.readInt();
			if (len <= 0) {
				return;
			}

			bt = new byte[len];
			in.readFully(bt, 0, len);
		} catch (Exception e) {
			return;
		}

		String[] vals = new String(bt).split(",");
		for (int i = 0; i < vals.length; i++) {
			String[] tp = vals[i].split(":", 2);
			if (tp.length == 2) {
				this.row.put(tp[0], tp[1]);
			}
		}
	}

	@Override
	public void write(DataOutput out, String[] fieldNames) throws IOException {
		this.write(out, fieldNames, ",".getBytes(), "\n".getBytes());
	}

	@Override
	public void write(DataOutput out, String[] fieldNames, byte[] fieldSplitChars) throws IOException {
		this.write(out, fieldNames, fieldSplitChars, "\n".getBytes());
	}

	@Override
	public void write(DataOutput out, String[] fieldNames, byte[] fieldSplitChars, byte[] rowSplitChars)
			throws IOException {
		if (fieldNames != null) {
			for (int i = 0; i < fieldNames.length; i++) {
				if (i > 0) {
					out.write(fieldSplitChars);
				}
				Object obj = this.row.get(fieldNames[i]);
				String val = null == obj ? "" : obj.toString();
				out.write(val.getBytes());
			}
		} else {
			int i = 0;
			for (String name : this.row.keySet()) {
				if (i++ > 0) {
					out.write(fieldSplitChars);
				}

				String val = this.row.get(name).toString();
				out.write(val.getBytes());
			}
		}
		out.write(rowSplitChars);
	}

	@Override
	public void write(DataOutput out, String[] fieldNames, byte[] fieldSplitChars, byte[] rowSplitChars,
			boolean isFisrtRow) throws IOException {
		if (!isFisrtRow) {
			out.write(rowSplitChars);
		}
		if (fieldNames != null) {
			for (int i = 0; i < fieldNames.length; i++) {
				if (i > 0) {
					out.write(fieldSplitChars);
				}
				Object obj = this.row.get(fieldNames[i]);
				String val = null == obj ? "" : obj.toString();
				out.write(val.getBytes());
			}
		} else {
			int i = 0;
			for (String name : this.row.keySet()) {
				if (i++ > 0) {
					out.write(fieldSplitChars);
				}

				String val = this.row.get(name).toString();
				out.write(val.getBytes());
			}
		}
	}

	@Override
	public void write(SequenceFile.Writer out, String[] fieldNames, String fieldSplitChars, String rowSplitChars)
			throws IOException {
		LongWritable idKey = new LongWritable(0);
		StringBuilder value = new StringBuilder();
		if (fieldNames != null) {
			for (int i = 0; i < fieldNames.length; i++) {
				if (i > 0) {
					value.append(fieldSplitChars);
				}

				Object obj = this.row.get(fieldNames[i]);
				value.append(null == obj ? "" : obj.toString());
			}
		} else {
			int i = 0;
			for (String name : this.row.keySet()) {
				if (i++ > 0) {
					value.append(fieldSplitChars);
				}

				Object obj = this.row.get(name);
				value.append(null == obj ? "" : obj.toString());
			}
		}
		out.append(idKey, new Text(value.toString()));
	}

	@Override
	public void write(RCFile.Writer out, String[] fieldNames, String fieldSplitChars, String rowSplitChars)
			throws IOException {
		// <WritableComparable, BytesRefArrayWritable>
		String temp = null;
		if (fieldNames != null) {
			BytesRefArrayWritable braw = new BytesRefArrayWritable(fieldNames.length);
			for (int i = 0; i < fieldNames.length; i++) {
				Object obj = this.row.get(fieldNames[i]);
				temp = null == obj ? "" : obj.toString();
				braw.set(i, new BytesRefWritable(temp.getBytes()));
			}
			out.append(braw);
		} else {
			BytesRefArrayWritable braw = new BytesRefArrayWritable(this.row.size());
			int i = 0;
			for (String name : this.row.keySet()) {
				Object obj = this.row.get(name);
				temp = null == obj ? "" : obj.toString();
				braw.set(i, new BytesRefWritable(temp.getBytes()));
			}
			out.append(braw);
		}
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		ResultSetMetaData RsMd = resultSet.getMetaData();
		int ColsCount = RsMd.getColumnCount();
		String[] fieldNames = new String[ColsCount];
		for (int n = 0; n < ColsCount; n++) {
			fieldNames[n] = RsMd.getColumnName(n);
		}
		readFields(resultSet, fieldNames);
	}

	@Override
	public void readFields(ResultSet resultSet, String[] fieldNames) throws SQLException {
		for (int i = 0; i < fieldNames.length; i++) {
			this.row.put(fieldNames[i], resultSet.getObject(i + 1));
		}
	}

	@Override
	public void readFields(DataInput in, String[] fieldNames) throws IOException {
		readFields(in, fieldNames, ",", "\n");
	}

	@Override
	public void readFields(DataInput in, String[] fieldNames, String splitChars) throws IOException {
		readFields(in, fieldNames, splitChars, "\n");
	}

	@Override
	public void readFields(DataInput in, String[] fieldNames, String splitChars, String rowSplitChars)
			throws IOException {
		this.row.clear();
		String val = in.readLine();
		val.replaceAll(rowSplitChars, "");
		String[] vals = val.split(splitChars);
		if (vals.length != fieldNames.length) {
			return;
		}

		for (int i = 0; i < fieldNames.length; i++) {
			this.row.put(fieldNames[i], vals[i]);
		}
	}

	// 写入到hbase表
	@Override
	public void write(HTable hTable, RuleConvert ruleConvertPO, String[][] clusterFieldNames,
			String[][] outColumnSplitRelation, String splitSign, HbaseRecordWriter<?, ?> hwriter) throws IOException {
		// 生成rowkey
		String rowKey = ruleConvertPO.getValue(this.row);
		if (null == rowKey) {
			return;
		}

		if (null != hwriter && hwriter.isUseOnlyFlag()) {
			rowKey += hwriter.getOnlyRowkey();
		}

		StringBuilder stbuf = null;
		Put put = new Put(Bytes.toBytes(rowKey));
		for (int i = 0; i < clusterFieldNames.length; i++) {
			String columnFamily[] = clusterFieldNames[i];
			String fields[] = outColumnSplitRelation[i];
			stbuf = new StringBuilder();
			for (int j = 0; j < fields.length; j++) {
				Object fieldValue = StringUtil.objectToString(this.row.get(fields[j]), "");
				stbuf.append(fieldValue);
				if (j < fields.length - 1) {
					stbuf.append(splitSign);
				}
			}

			put.add(Bytes.toBytes(columnFamily[0]), Bytes.toBytes(columnFamily[1]), Bytes.toBytes(stbuf.toString()));
			// if (null != rowKey && (columnFamily.length == 2 ||
			// columnFamily.length == 3) && null != columnFamily[0]
			// && null != columnFamily[1]) {
			// Put put = new Put(Bytes.toBytes(rowKey));
			// if (columnFamily.length == 3 && null != columnFamily[2]) {
			// if ("NULL".equalsIgnoreCase(columnFamily[2])) {
			// put.add(Bytes.toBytes(columnFamily[0]),
			// Bytes.toBytes(columnFamily[1]), Bytes.toBytes(""));
			// hTable.put(put);
			// hwriter.heapSize(put);
			// } else {
			// put.add(Bytes.toBytes(columnFamily[0]),
			// Bytes.toBytes(columnFamily[1]), Bytes.toBytes(columnFamily[2]));
			// hTable.put(put);
			// hwriter.heapSize(put);
			// }
			//
			// continue;
			// }
			//
			// Object fieldValue = this.row.get(outputFieldNames[i]);
			// if (null == fieldValue) {
			// continue;
			// }
			// put.add(Bytes.toBytes(columnFamily[0]),
			// Bytes.toBytes(columnFamily[1]),
			// Bytes.toBytes(fieldValue.toString()));
			// hTable.put(put);
			// hwriter.heapSize(put);
			// }
		}
		hwriter.setDurability(put);
		hTable.put(put);
		// hwriter.heapSize(put);
	}

	// 删除hbase表数据
	@Override
	public void delete(List<Delete> lstDel, RuleConvert ruleConvertPO, String[] fieldNames) {
		// 生成rowkey
		String rowKey = ruleConvertPO.getValue(this.row);
		if (null == rowKey) {
			return;
		}
		Delete del = new Delete(Bytes.toBytes(rowKey));
		lstDel.add(del);
	}

	// 读取hbase表中的字段
	@Override
	public void readFields(KeyValue[] keyValue) throws IOException {
		readFields(keyValue, null, null);
	}

	// 读取hbase表中的字段
	@Override
	public void readFields(KeyValue[] keyValue, String splitSign, Map<String, String[]> srcTargetFiledNameMap)
			throws IOException {
		this.row.clear();
		if (null == keyValue) {
			return;
		}
		for (KeyValue kv : keyValue) {
			String family = new String(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
			String column = new String(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
			String value = new String(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
			if (null != srcTargetFiledNameMap && srcTargetFiledNameMap.size() > 0) {
				String targetFiled[] = srcTargetFiledNameMap.get(family + HbaseConfiguration.sign_lineae + column);
				if (null != targetFiled && null != splitSign) {
					String fVar[] = value.split(splitSign, targetFiled.length);
					for (int i = 0; i < targetFiled.length; i++) {
						this.row.put(targetFiled[i], (i + 1 > fVar.length) ? "" : fVar[i]);
					}
				} else if (null != targetFiled && targetFiled.length == 1) {// 特殊只有一个字段
					this.row.put(targetFiled[0], targetFiled);
				}
			} else {
				this.row.put(column, value);
			}
		}
	}

	/**
	 * 当前记录是否为空
	 * 
	 * @return true表示为空
	 */
	public boolean isNull() {
		return this.row.size() == 0;
	}

	@Override
	public int compare(DBRecord o1, DBRecord o2) {
		if (o1 != null && o2 != null) {
			return o1.compareTo(o2);
		} else if (o1 == null) {
			return -1;
		} else {
			return 1;
		}
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int pos = 0;
		while (true) {
			if (l1 > s1 + pos && l2 > s2 + pos) {
				int c = b1[pos + s1] - b2[s2 + pos];
				if (c != 0) {
					return c;
				}
				pos++;
				continue;
			} else if (l1 < s1 + pos) {
				return -1;
			} else if (l2 < s2 + pos) {
				return 1;
			}
			return 0;
		}
	}
}
