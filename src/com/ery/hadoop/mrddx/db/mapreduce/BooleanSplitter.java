package com.ery.hadoop.mrddx.db.mapreduce;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * 

 * 

 * @Comments 实现 DBSplitter接口的Boolean类. 处理数据类型：java.sql.Types (BIT,BOOLEAN)

 * @version v1.0
 * @create Data 2013-1-9
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BooleanSplitter implements DBSplitter {
	@Override
	public List<InputSplit> split(Configuration conf, ResultSet results, String colName) throws SQLException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		// 1:最大最小都为null时，不进行拆分
		if (results.getString(1) == null && results.getString(2) == null) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL"));
			return splits;
		}

		boolean minVal = results.getBoolean(1);
		boolean maxVal = results.getBoolean(2);
		// Use one or two splits.
		if (!minVal) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " = FALSE", colName + " = FALSE"));
		}

		if (maxVal) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " = TRUE", colName + " = TRUE"));
		}

		if (results.getString(1) == null || results.getString(2) == null) {
			// Include a null value.
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL"));
		}

		return splits;
	}
}
