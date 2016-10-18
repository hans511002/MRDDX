package com.ery.hadoop.mrddx.db.partitiondb.route;

import com.ery.hadoop.mrddx.util.Pair;
import com.ery.hadoop.mrddx.util.PairUtil;
import com.ery.hadoop.mrddx.util.StringUtil;

public final class PartitionByString extends PartitionFunction {
	public PartitionByString(int[] count, int[] length, String hashSlice) {
		super(count, length);
		setHashSlice(hashSlice);
	}

	private int hashSliceStart = 0;
	/** 0 means str.length(), -1 means str.length()-1 */
	private int hashSliceEnd = 8;

	public void setHashLength(int hashLength) {
		setHashSlice(String.valueOf(hashLength));
	}

	public void setHashSlice(String hashSlice) {
		Pair<Integer, Integer> p = PairUtil.sequenceSlicing(hashSlice);
		hashSliceStart = p.getKey();
		hashSliceEnd = p.getValue();
	}

	@Override
	public Integer calculate(Object arg) {
		if (arg == null) {
			return 0;
		}
		String key = String.valueOf(arg);
		int start = hashSliceStart >= 0 ? hashSliceStart : key.length() + hashSliceStart;
		int end = hashSliceEnd > 0 ? hashSliceEnd : key.length() + hashSliceEnd;
		long hash = StringUtil.hash(key, start, end);
		return partitionIndex(hash);
	}

}
