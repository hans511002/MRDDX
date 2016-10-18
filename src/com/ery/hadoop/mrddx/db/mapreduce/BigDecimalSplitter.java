package com.ery.hadoop.mrddx.db.mapreduce;

import java.math.BigDecimal;
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
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * Copyrights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 实现 DBSplitter接口的BigDecimal类.
 *           处理数据类型：java.sql.Types.NUMERIC，java.sql.Types.DECIMAL
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BigDecimalSplitter implements DBSplitter {
	private static final Log LOG = LogFactory.getLog(BigDecimalSplitter.class);
	private static final BigDecimal MIN_INCREMENT = new BigDecimal(1);

	@Override
	public List<InputSplit> split(Configuration conf, ResultSet results, String colName) throws SQLException {
		BigDecimal minVal = results.getBigDecimal(1);
		BigDecimal maxVal = results.getBigDecimal(2);

		String lowClausePrefix = colName + " >= ";
		String highClausePrefix = colName + " < ";

		BigDecimal numSplits = new BigDecimal(conf.getInt(MRConfiguration.MAPRED_MAP_TASKS, 1));
		// 1:最大最小都为null时，不进行拆分
		if (minVal == null && maxVal == null) {
			List<InputSplit> splits = new ArrayList<InputSplit>();
			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL"));
			return splits;
		}

		// 2:不能找到合理的最大最小值
		if (minVal == null || maxVal == null) {
			LOG.error("Cannot find a range for NUMERIC or DECIMAL fields with one end NULL.");
			return null;
		}

		// 3:获取拆分列表
		List<BigDecimal> splitPoints = this.split(numSplits, minVal, maxVal);// 获取拆分点
		List<InputSplit> splits = new ArrayList<InputSplit>();

		// Turn the split points into a set of intervals.
		BigDecimal start = splitPoints.get(0);
		for (int i = 1; i < splitPoints.size(); i++) {
			BigDecimal end = splitPoints.get(i);

			String low = lowClausePrefix + start.toString();
			String high = null;
			if (i == splitPoints.size() - 1) {
				// This is the last one; use a closed interval.
				high = colName + " <= " + end.toString();
			} else {
				// Normal open-interval case.
				high = highClausePrefix + end.toString();
			}

			splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(low, high));
			start = end;
		}

		return splits;
	}

	/**
	 * Returns a list of BigDecimals one element longer than the list of input
	 * splits. This represents the boundaries between input splits. All splits
	 * are open on the top end, except the last one. 例如：[0, 5, 8, 12, 18]
	 * 使用区间表示: [0, 5) [5, 8) [8, 12) [12, 18] ,注意:开闭区间
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
	protected List<BigDecimal> split(BigDecimal numSplits, BigDecimal minVal, BigDecimal maxVal) throws SQLException {
		List<BigDecimal> splits = new ArrayList<BigDecimal>();
		// 获取拆分数量
		BigDecimal splitSize = this.tryDivide(maxVal.subtract(minVal), numSplits);
		if (splitSize.compareTo(MIN_INCREMENT) < 0) {
			splitSize = MIN_INCREMENT;
			MRLog.warn(LOG, "Set BigDecimal splitSize to MIN_INCREMENT");
		}

		BigDecimal curVal = minVal;
		while (curVal.compareTo(maxVal) <= 0) {
			splits.add(curVal);
			curVal = curVal.add(splitSize);
		}

		// We didn't end on the maxVal. Add that to the end of the list.
		if (splits.get(splits.size() - 1).compareTo(maxVal) != 0 || splits.size() == 1) {
			splits.add(maxVal);
		}

		return splits;
	}

	/**
	 * numerator除以denominator, 如果无法在精确模式下，使用"四舍五入"。
	 * 
	 * @param numerator
	 *            为最大值减去最小值
	 * @param denominator
	 *            map任务数
	 * @return 拆分数量
	 */
	protected BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
		try {
			return numerator.divide(denominator);
		} catch (ArithmeticException ae) {
			return numerator.divide(denominator, BigDecimal.ROUND_HALF_UP);
		}
	}
}
