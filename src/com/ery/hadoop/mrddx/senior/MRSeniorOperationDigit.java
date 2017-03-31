package com.ery.hadoop.mrddx.senior;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.RuleConvert;

/**
 * 实现MR的高级应用接口类，处理运算:+ - * / ; 结果为浮点类型 语法规则：({a}+1)/2-5+{b}*3+{b}%2={c}
 * 



 * @createDate 2013-2-18
 * @version v1.0
 */
public class MRSeniorOperationDigit implements IMRSeniorApply {
	public static final Log LOG = LogFactory.getLog(MRSeniorOperationDigit.class);
	private static final java.util.regex.Pattern FLOAT_PATTERN = Pattern.compile("[0-9]*(\\.?)[0-9]*");
	private static Pattern PATTERN_INT = Pattern.compile("^-?\\d+$");
	private static Pattern PATTERN_FLOAT = Pattern.compile("^(-?\\d+)(\\.\\d+)?$");

	private String rule;
	private String opera;
	private String rsname;
	private Expression compiledExp;
	private Map<String, Object> objMap;
	private boolean status = true; // 解析规则状态，true：正确， false：错误

	public MRSeniorOperationDigit(Configuration conf, int mapOrReduce) {
	}

	@Override
	public void initRule(String rule) {
		this.rule = rule;
		if (null == this.rule || this.rule.trim().length() <= 0 || this.rule.indexOf("=") == -1
				|| this.rule.indexOf("=") + 1 == this.rule.length()) {
			this.status = false;
			return;
		}

		this.opera = this.rule.substring(0, this.rule.indexOf("="));
		this.rsname = this.rule.substring(rule.indexOf("=") + 1, rule.length());
		if (null != this.rsname && this.rsname.startsWith("{") && this.rsname.endsWith("}")) {
			this.rsname = this.rsname.substring(1, this.rsname.length() - 1);
		} else {
			this.status = false;
			return;
		}

		String exp = this.opera.replaceAll("\\{", "");
		exp = exp.replaceAll("\\}", "");
		System.out.println("Digit exp=" + exp);
		Pattern pattern2 = Pattern.compile("\\{(.*?)\\}");
		Matcher matcher = pattern2.matcher(this.opera);
		this.objMap = new HashMap<String, Object>();
		while (matcher.find()) {
			for (int i = 0; i < matcher.groupCount(); i++) {
				String temp = matcher.group(i);
				if (null != temp && temp.startsWith("{") && temp.endsWith("}")) {
					this.objMap.put(temp.substring(1, temp.length() - 1), "");
				}
			}
		}
		// 编译表达式
		this.compiledExp = AviatorEvaluator.compile(exp);
	}

	@Override
	public void apply(DBRecord dbRecord, Object obj) {
		if (null == dbRecord || dbRecord.isNull()) {
			return;
		}

		Map<String, Object> row = dbRecord.getRow();
		if (!this.status) {
			return;
		}

		// 计算语句
		for (String key : this.objMap.keySet()) {
			String ob = row.get(key).toString();
			if (PATTERN_INT.matcher(ob).matches()) {
				this.objMap.put(key, Integer.parseInt(ob));
			} else if (PATTERN_FLOAT.matcher(ob).matches()) {
				this.objMap.put(key, Float.parseFloat(ob));
			} else {
				this.objMap.put(key, ob);
			}
		}

		// 执行表达式
		try {
			Object result = this.compiledExp.execute(this.objMap);
			row.put(this.rsname, result);
		} catch (Exception e) {
			row.put(this.rsname, "");
			MRLog.warnException(LOG, "Execute_Evaluator:" + opera, e);
		}
	}

	public void apply(DBRecord dbRecord) {
		if (null == dbRecord || dbRecord.isNull() || null == this.rule || this.rule.trim().length() <= 0
				|| this.rule.indexOf("=") == -1) {
			return;
		}

		String rsname = this.rule.substring(rule.indexOf("=") + 1, rule.length());
		if (rsname.trim().length() <= 0) {
			return;
		}

		// 计算语句
		String opera = this.rule.substring(0, this.rule.indexOf("="));
		Map<String, Object> row = dbRecord.getRow();
		Iterator<String> iterator = row.keySet().iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			Object value = row.get(key);
			if (null == key || key.trim().length() <= 0 || null == value) {
				continue;
			}

			String v = String.valueOf(value);
			if (!this.isDecimal(v)) {
				continue;
			}

			opera = opera.replaceAll(key, v);
		}

		try {
			Object result = AviatorEvaluator.execute(opera);
			row.put(rsname, result);
		} catch (Exception e) {
			MRLog.warnException(LOG, "Execute_Evaluator:" + opera, e);
		}
	}

	public boolean isDecimal(String str) {
		if (str == null || "".equals(str)) {
			return false;
		}

		return FLOAT_PATTERN.matcher(str).matches();
	}

	public static void main(String[] args) {
		// testget1();
		// testget2();
		// testget3();
		testget4();
	}

	private static long testget1() {
		Map<String, Object> env = new HashMap<String, Object>();
		env.put("a", "2");
		env.put("b", "5");
		long time = System.currentTimeMillis();
		String rule = "({a}+1)/2-5+{b}*3+{b}%2";
		RuleConvert rc = new RuleConvert(rule);
		for (int i = 0; i < 1000000; i++) {
			rule = rc.getValue(env);
			if (i % 100000 == 0) {
				System.out.println(rule);
			}
			Object obj = AviatorEvaluator.execute(rule);
			if (i % 100000 == 0) {
				System.out.println(obj);
			}
		}
		System.out.println("condition1:" + (System.currentTimeMillis() - time));
		return time;
	}

	private static void testget2() {
		long time = System.currentTimeMillis();
		MRSeniorOperationDigit digit = new MRSeniorOperationDigit(null, 0);
		String expression1 = "(a+1)/2-5+b*3+b%2=c"; // 编译表达式
		digit.initRule(expression1);
		DBRecord dbRecord = new DBRecord();
		for (int i = 0; i < 1000000; i++) {
			dbRecord.putData("a", 2.00);
			dbRecord.putData("b", 5.2313);
			digit.apply(dbRecord);
			if (i % 100000 == 0) {
				System.out.println(dbRecord.getData("c"));
			}
		}
		System.out.println("condition2:" + (System.currentTimeMillis() - time));
	}

	private static void testget3() {
		String expression1 = "({a}+1)/2-5+{b}*3+{b}%2"; // 编译表达式
		String expression2 = expression1.replaceAll("\\{", ""); // 编译表达式
		expression2 = expression2.replaceAll("\\}", ""); // 编译表达式
		System.out.println("expression2=" + expression2);
		Pattern pattern2 = Pattern.compile("\\{(.*?)\\}");
		Matcher matcher = pattern2.matcher(expression1);
		Map<String, Object> objMap = new HashMap<String, Object>();
		while (matcher.find()) {
			for (int i = 0; i < matcher.groupCount(); i++) {
				String temp = matcher.group(i);
				if (null != temp && temp.startsWith("{") && temp.endsWith("}")) {
					System.out.println(temp.substring(1, temp.length() - 1));
					objMap.put(temp.substring(1, temp.length() - 1), "");
				}
			}
		}

		Expression compiledExp = AviatorEvaluator.compile(expression2);
		Map<String, Object> env1 = new HashMap<String, Object>();
		env1.put("a", 2.00);
		env1.put("b", 5.2313);

		Pattern patternInt = Pattern.compile("^-?\\d+$");
		Pattern patternFloat = Pattern.compile("^(-?\\d+)(\\.\\d+)?$");
		long time = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			for (String key : objMap.keySet()) {
				String ob = env1.get(key).toString();
				if (patternInt.matcher(ob).matches()) {
					objMap.put(key, Integer.parseInt(ob));
				} else if (patternFloat.matcher(ob).matches()) {
					objMap.put(key, Float.parseFloat(ob));
				} else {
					objMap.put(key, ob);
				}
			}

			// 执行表达式
			Object result = compiledExp.execute(objMap);
			if (i % 100000 == 0) {
				System.out.println(result);
			}
		}
		System.out.println("condition3:" + (System.currentTimeMillis() - time));
	}

	private static void testget4() {
		long time = System.currentTimeMillis();
		MRSeniorOperationDigit digit = new MRSeniorOperationDigit(null, 0);
		String expression1 = "({a}+1)/2-5+{b}*3+{b}%2={c}"; // 编译表达式
		digit.initRule(expression1);
		DBRecord dbRecord = new DBRecord();
		for (int i = 0; i < 1000000; i++) {
			dbRecord.putData("a", 2.00);
			dbRecord.putData("b", 5);
			digit.apply(dbRecord, null);
			if (i % 100000 == 0) {
				System.out.println(dbRecord.getData("c"));
			}
		}
		System.out.println("condition4:" + (System.currentTimeMillis() - time));
	}
}
