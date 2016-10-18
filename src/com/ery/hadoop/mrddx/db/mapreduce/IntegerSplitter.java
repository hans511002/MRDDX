package com.ery.hadoop.mrddx.db.mapreduce;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.ery.hadoop.mrddx.MRConfiguration;

/**
 * 
 * Copyrights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 实现 DBSplitter接口的Integer类. 处理数据类型：java.sql.Types (INTEGER TINYINT
 *           SMALLINT BIGINT)
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IntegerSplitter implements DBSplitter {
	@Override
	public List<InputSplit> split(Configuration conf, ResultSet results, String colName) throws SQLException {
		long minVal = results.getLong(1);
		long maxVal = results.getLong(2);
		String lowClausePrefix = colName + " >= ";
		String highClausePrefix = colName + " < ";

		int numSplits = conf.getInt(MRConfiguration.MAPRED_MAP_TASKS, 1);
		if (numSplits < 1) {
			numSplits = 1;
		}

		// 1:最大最小都为null时，不进行拆分
		if (results.getString(1) == null && results.getString(2) == null) {
			List<InputSplit> splits = new ArrayList<InputSplit>();
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL"));
			return splits;
		}

		// 2:获取拆分列表
		List<Long> splitPoints = this.split(numSplits, minVal, maxVal);// 获取拆分点列表
		List<InputSplit> splits = new ArrayList<InputSplit>();

		// Turn the split points into a set of intervals.
		long start = splitPoints.get(0);
		for (int i = 1; i < splitPoints.size(); i++) {
			long end = splitPoints.get(i);

			String low = lowClausePrefix + Long.toString(start);
			String high = null;
			if (i == splitPoints.size() - 1) {
				// This is the last one; use a closed interval.
				high = colName + " <= " + Long.toString(end);
			} else {
				// Normal open-interval case.
				high = highClausePrefix + Long.toString(end);
			}

			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(low, high));
			start = end;
		}

		// 3:如果某个列的值为null，则添加a null split.
		if (results.getString(1) == null || results.getString(2) == null) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL"));
		}

		return splits;
	}

	/**
	 * Returns a list of longs one element longer than the list of input splits.
	 * This represents the boundaries between input splits. All splits are open
	 * on the top end, except the last one. 例如：[0, 5, 8, 12, 18] 使用区间表示: [0, 5)
	 * [5, 8) [8, 12) [12, 18] ,注意:开闭区间
	 * 
	 * @param numSplits
	 *            map任务数
	 * @param minVal
	 *            最小值
	 * @param maxVal
	 *            最大值
	 * @return 拆分列表
	 * @throws SQLException
	 */
	protected List<Long> split(long numSplits, long minVal, long maxVal) throws SQLException {
		List<Long> splits = new ArrayList<Long>();

		// Use numSplits as a hint. May need an extra task if the size doesn't
		// divide cleanly.
		long splitSize = (maxVal - minVal) / numSplits;
		if (splitSize < 1) {
			splitSize = 1;
		}

		long curVal = minVal;
		while (curVal <= maxVal) {
			splits.add(curVal);
			curVal += splitSize;
		}

		if (splits.get(splits.size() - 1) != maxVal || splits.size() == 1) {
			// We didn't end on the maxVal. Add that to the end of the list.
			splits.add(maxVal);
		}

		return splits;
	}
}
