package com.ery.hadoop.mrddx.db.mapreduce;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.ery.hadoop.mrddx.MRConfiguration;

/**
 * Implement DBSplitter over floating-point values. Copyrights @
 * 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 实现 DBSplitter接口的floating-point类. 处理数据类型：java.sql.Types
 *           (REAL,FLOAT,DOUBLE)
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FloatSplitter implements DBSplitter {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(FloatSplitter.class);
	private static final double MIN_INCREMENT = 10000 * Double.MIN_VALUE;

	@Override
	public List<InputSplit> split(Configuration conf, ResultSet results, String colName) throws SQLException {
		LOG.warn("Generating splits for a floating-point index column. Due to the");
		LOG.warn("imprecise representation of floating-point values in Java, this");
		LOG.warn("may result in an incomplete import.");
		LOG.warn("You are strongly encouraged to choose an integral split column.");

		List<InputSplit> splits = new ArrayList<InputSplit>();
		// 1:最大最小都为null时，不进行拆分
		if (results.getString(1) == null && results.getString(2) == null) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL"));
			return splits;
		}

		// 2:添加拆分列表
		double minVal = results.getDouble(1);
		double maxVal = results.getDouble(2);
		int numSplits = conf.getInt(MRConfiguration.MAPRED_MAP_TASKS, 1);
		double splitSize = (maxVal - minVal) / (double) numSplits;
		if (splitSize < MIN_INCREMENT) {
			splitSize = MIN_INCREMENT;
		}

		String lowClausePrefix = colName + " >= ";
		String highClausePrefix = colName + " < ";
		double curLower = minVal;
		double curUpper = curLower + splitSize;
		double tempcurUpper = curUpper;
		while (curUpper < maxVal) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix + Double.toString(curLower),
					highClausePrefix + Double.toString(curUpper)));

			curLower = curUpper;
			tempcurUpper = curUpper;
			curUpper += splitSize;
		}

		// 添加最后一个拆分(注意是闭区间结尾)
		if (curLower <= maxVal || splits.size() == 1) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
					+ Double.toString(tempcurUpper), colName + " <= " + Double.toString(maxVal)));
		}

		// 3:如果某个列的值为null，则添加a null split.
		if (results.getString(1) == null || results.getString(2) == null) {
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL"));
		}

		return splits;
	}
}
