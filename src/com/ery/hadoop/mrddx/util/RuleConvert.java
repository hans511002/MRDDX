package com.ery.hadoop.mrddx.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.hadoop.mrddx.hbase.HBaseRowKeyRule;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * sd-{fdsa}-{fdsa}--{date,yyyy-MM-dd}
 * 

 * 
 */
public class RuleConvert {
	private String[] valueArray;
	private String[][] macroVarArray;// 存放宏变量 index-{aaa}
	private String[][] macroSepcilVarArray;// 存放宏变量 index-{date,yyyy-MM-dd}
	boolean isExecExp = false;
	Expression expr = null;
	Map<String, Boolean> varNames = null;

	public RuleConvert(String ruleValue) {
		this.decodeRule(ruleValue);
	}

	/**
	 * 使用数组解析
	 * 
	 * @param ruleValue
	 */
	private void decodeRule(String ruleValue) {
		if (null == ruleValue || ruleValue.trim().length() <= 0) {
			return;
		}
		if (ruleValue.startsWith("exec:")) {
			expr = AviatorEvaluator.compile(ruleValue.substring(5).replaceAll("\\{|\\}", ""), true);
			varNames = new HashMap<>();
			for (String var : expr.getVariableNames()) {
				varNames.put(var, true);
			}
			isExecExp = true;
			return;
		}

		// 存放字段的值，例如 key={aaa} value=123
		Map<String, String> macroVariableMap = new HashMap<String, String>();
		// 存放特殊宏变量与字段关系 key={date,yyyy-MM-dd} value={yyyy-MM-dd}
		Map<String, String> tempMacroVariableMap = new HashMap<String, String>();
		// 存放字段与特殊格式化的关系 key={date} value={yyyy-MM-dd}
		Map<String, String> specilMacroVariableMap = new HashMap<String, String>();
		List<String> lstValue = new ArrayList<String>();
		List<String> lstGroup = new ArrayList<String>();

		Pattern pattern2 = Pattern.compile("\\{(.*?)\\}");
		Matcher matcher = pattern2.matcher(ruleValue);
		while (matcher.find()) {
			for (int i = 0; i < matcher.groupCount(); i++) {
				String temp = matcher.group(i);
				lstGroup.add(temp);
				String s[] = temp.split(",");
				if (s.length == 1) {
					macroVariableMap.put(temp, "");
				} else {
					macroVariableMap.put(s[0] + "}", "");
					specilMacroVariableMap.put(s[0] + "}", s[1].substring(0, s[1].length() - 1));
					tempMacroVariableMap.put(temp, s[0] + "}");
				}
			}
		}

		String temp = ruleValue;
		int tmpIndex = -1;
		for (int i = 0; i < lstGroup.size(); i++) {
			String tmp = lstGroup.get(i);
			tmpIndex = temp.indexOf(tmp);
			String s = temp.substring(0, tmpIndex);
			if (s.length() != 0) {
				lstValue.add(s);
			}

			lstValue.add(tmp);
			temp = temp.substring(tmpIndex + tmp.length());
			if (lstGroup.size() - 1 == i && temp.length() > 0) {
				lstValue.add(temp);
			}
		}

		this.valueArray = new String[lstValue.size()];
		this.macroVarArray = new String[lstValue.size()][];
		this.macroSepcilVarArray = new String[lstValue.size()][];
		int mavarIndex = 0;
		int specilMavarIndex = 0;
		for (int i = 0; i < lstValue.size(); i++) {
			this.valueArray[i] = lstValue.get(i);
			if (macroVariableMap.containsKey(this.valueArray[i])) {
				this.macroVarArray[i] = new String[] { this.valueArray[i].substring(1, this.valueArray[i].length() - 1) };
				mavarIndex++;
			}

			if (tempMacroVariableMap.containsKey(this.valueArray[i])) {
				String var = tempMacroVariableMap.get(this.valueArray[i]);
				String tvar = specilMacroVariableMap.get(var);
				this.macroSepcilVarArray[i] = new String[] { var.substring(1, var.length() - 1), tvar };
				specilMavarIndex++;
			}
		}
	}

	/**
	 * 使用数组解析， 替换值
	 * 
	 * @param fieldValue
	 */
	public String getValue(Map<String, Object> fieldValue) {
		if (isExecExp) {
			try {
				return expr.execute(fieldValue).toString();
			} catch (Exception e) {
				MRLog.systemOut("exec RowKEY error " + e.getMessage() + "  fieldValues=" + fieldValue);
				return null;
			}
		}
		StringBuilder strBuilder = new StringBuilder();
		String temp = "";
		for (int i = 0; i < valueArray.length; i++) {
			temp = valueArray[i];
			String tmp[] = this.macroVarArray[i];
			if (null != tmp) {
				temp = StringUtil.objectToString(fieldValue.get(tmp[0]), temp);
				strBuilder.append(temp);
				continue;
			}

			String stmp[] = this.macroSepcilVarArray[i];
			if (null != stmp) {
				String var = StringUtil.objectToString(fieldValue.get(stmp[0]), null);
				if (null != var) {
					temp = HBaseRowKeyRule.getValueByRule(var, stmp[1]);
				}
			}

			strBuilder.append(temp);
		}
		return strBuilder.toString();
	}

	public static void main(String[] args) {
		String ruleValue = "filepath +'-'+ currentvalue+'currentvalue'+ sss +'--'+ada+'{}{sa{sad}'"
				+ "--date_to_string(string_to_date(DATE ,'yyyy-MM-dd HH:mm:ss') ,'yyyy-MM-dd')";
		ruleValue = "filepath+'-'+currentvalue+'currentvalue'+ sss +'--'+ada+'{}{sa{sad}"
				+ "--'+string.substring(DATE,0,10)";
		// +
		// "+'--'+date_to_string(string_to_date(DATE ,'yyyy-MM-dd HH:mm:ss') ,'yyyy-MM-dd')";
		// String ruleValue =
		// "{filepath}-{currentvalue}currentvalue{sss}--{ada}{}{sa{sad}--{DATE,yyyy-MM-dd}";
		Expression expr = AviatorEvaluator.compile(ruleValue.replaceAll("\\{|\\}", ""), true);
		// string_to_date(source,format) date_to_string(date,format)
		Map<String, Object> fieldValue = new HashMap<String, Object>();
		fieldValue.put("filepath", "http://sina.com.cn");
		fieldValue.put("currentvalue", "1891");
		fieldValue.put("ada", "2222");
		fieldValue.put("sss", "44444");
		fieldValue.put("DATE", "2013-09-08 23:10:23");

		RuleConvert rcpo = new RuleConvert(ruleValue);
		long begin = System.currentTimeMillis();
		System.out.println(begin);
		for (int i = 0; i < 10000000; i++) {
			if (i % 1000000 == 0) {
				MRLog.systemOut(expr.execute(fieldValue).toString());
			}
			expr.execute(fieldValue);
			// rcpo.getValue(fieldValue);
		}
		long end = System.currentTimeMillis();
		System.out.println(end - begin);

		// Set<String> rowRuleList = rcpo.getRowRuleList(ruleValue);
		// rowRuleList.add("DATE,yyyy-MM-dd");
		// long begin = System.currentTimeMillis();
		// MRLog.systemOut(begin);
		// for (int i = 0; i < 100000; i++) {
		// if (i%10000 == 0){
		// MRLog.systemOut(rcpo.test(ruleValue, rowRuleList, new
		// String[]{"f1:filepath","f1:currentvalue","f1:sss","f1:DATE"},
		// fieldValue));
		// }
		// rcpo.test(ruleValue, rowRuleList, new
		// String[]{"f1:filepath","f1:currentvalue","f1:sss","f1:DATE"},
		// fieldValue);
		// }
		//
		// long end = System.currentTimeMillis();
		// MRLog.systemOut(end-begin);
	}
	// public String test(String rowKey, Set<String> rowRuleList, String[]
	// fieldNames, Map<String, String> row) {
	// // 生成rowkey
	// Iterator<String> iterator = rowRuleList.iterator();
	// while (iterator.hasNext()) {
	// String str = iterator.next();
	// for (int i = 0; i < fieldNames.length; i++) {
	// String columnFamily[] = fieldNames[i].split(":");
	// String tmp[] = str.split(",");
	// if (tmp.length == 1 && columnFamily[1].equalsIgnoreCase((tmp[0]))) {
	// Object fieldValue = row.get(tmp[0].trim());
	// if (null == fieldValue) {
	// fieldValue = "";
	// }
	// rowKey = rowKey.replaceAll("\\{" + str + "\\}", fieldValue.toString());
	// } else if (tmp.length == 2 && columnFamily[1].equalsIgnoreCase((tmp[0])))
	// {
	// Object fieldValue = row.get(tmp[0]);
	// if (null == fieldValue) {
	// fieldValue = "";
	// }
	// String value = HBaseRowKeyRule.getValueByRule(fieldValue.toString(),
	// tmp[1]);
	// rowKey = rowKey.replaceAll("\\{" + str + "\\}", value);
	// }
	// }
	// }
	//
	// return rowKey;
	// }
	//
	// /**
	// * 获取行规则
	// *
	// * @return 行规则
	// */
	// private Set<String> getRowRuleList(String rowKeyRule) {
	// Set<String> set = new HashSet<String>();
	//
	// if (null == rowKeyRule) {
	// return set;
	// }
	// int bIndex = -1;
	// int eIndex = -1;
	// String temp = null;
	// while (true) {
	// bIndex = rowKeyRule.indexOf("{", eIndex + 1);
	// eIndex = rowKeyRule.indexOf("}", eIndex + 1);
	// if (bIndex != -1 && eIndex != -1 && eIndex > (bIndex + 1)) {
	// temp = rowKeyRule.substring(bIndex + 1, eIndex);
	// set.add(temp);
	// } else {
	// break;
	// }
	// }
	//
	// return set;
	// }
}
