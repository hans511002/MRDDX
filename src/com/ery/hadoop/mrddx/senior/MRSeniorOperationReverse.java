package com.ery.hadoop.mrddx.senior;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.base.support.utils.MapUtils;

/**
 * 实现MR的高级应用接口类，反转数据 ; 结果为String类型 语法规则：{phone}:reverse
 * 语法规则：reverse:{phone}:{name}:{time}
 * 



 * @createDate 2013-11-20
 * @version v1.0
 */
public class MRSeniorOperationReverse implements IMRSeniorApply {
	private String[] reverseStr;// 要反转的目标
	private boolean status = true;

	public MRSeniorOperationReverse(Configuration conf, int mapOrReduce) {

	}

	@Override
	public void apply(DBRecord dbRecord, Object obj) {
		if (null == dbRecord || dbRecord.isNull()) {
			return;
		}
		if (!this.status) {
			return;
		}
		Map<String, Object> row = dbRecord.getRow();
		for (String reStr : this.reverseStr) {
			if (row.containsKey(reStr)) {
				row.put(reStr, reverseString(MapUtils.getString(row, reStr, "")));
			}
		}
	}

	@Override
	public void initRule(String reverseStr) {
		if (null == reverseStr) {
			this.status = false;
			return;
		}

		String reverseParam[] = reverseStr.split(":");
		this.reverseStr = new String[reverseParam.length];
		for (int i = 0; i < reverseParam.length; i++) {
			String str = reverseParam[i];
			if (str != null && str.length() > 0) {
				this.reverseStr[i] = (undock(str));
			}
		}
	}

	private String undock(String kvarry) {
		if (null != kvarry && kvarry.startsWith("{") && kvarry.endsWith("}")) {
			return kvarry = kvarry.substring(1, kvarry.length() - 1);
		} else {
			return kvarry;
		}
	}

	public static void main(String[] args) {
		Map<String, Object> row = new HashMap<String, Object>();
		row.put("filepath", null);
		row.put("name", "fewfwe");
		row.put("pas", null);
		row.put("phone", "asdf#23");

		DBRecord dbRecord = new DBRecord(row);
		MRSeniorOperationReverse m = new MRSeniorOperationReverse(null, 0);
		m.initRule("{phone}:{name}:{pas}");
		long time = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			m.apply(dbRecord, "");
			if (i % 1000000 == 0) {
				// System.out.println(dbRecord.getData("phone"));
				// System.out.println(dbRecord.getData("name"));
				System.out.println(dbRecord.getData("pas"));
			}
		}
		System.out.println("condition:" + (System.currentTimeMillis() - time));
		System.out.println(dbRecord.toString());
	}

	private String reverseString(String str) {
		char[] chars = str.toCharArray();
		char[] nechars = new char[chars.length];
		for (int j = chars.length - 1; j >= 0; j--) {
			nechars[chars.length - j - 1] = chars[j];
		}
		return String.valueOf(nechars);
	}

}
