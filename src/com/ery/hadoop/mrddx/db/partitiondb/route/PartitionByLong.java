package com.ery.hadoop.mrddx.db.partitiondb.route;

public final class PartitionByLong extends PartitionFunction {
	public PartitionByLong(int[] count, int length[]) {
		super(count, length);
	}

	@Override
	public Integer calculate(Object arg) {
		Number key;
		if (arg == null) {
			return 0;
		} else if (arg instanceof Number) {
			key = (Number) arg;
		} else if (arg instanceof String) {
			key = Long.parseLong((String) arg);
		} else {
			throw new IllegalArgumentException("unsupported data type for partition key: " + arg.getClass());
		}
		return partitionIndex(key.longValue());
	}

}
