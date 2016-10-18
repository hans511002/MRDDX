package com.ery.hadoop.mrddx.example;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobPriority;

import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils.FileType;

/**
 * 服务抽象类 Copyrights @ 2012,Tianyuan DIC Information Co.,Ltd. All rights
 * reserved.
 * 
 * @author wanghao
 * @version v1.0
 */
public class MRTestJOBService {
	public static final Log LOG = LogFactory.getLog(MRTestJOBService.class);
	public static final String hiveDriverClass = "org.apache.hadoop.hive.jdbc.HiveDriver";

	// 获取公共参数
	/**
	 * 获取MR最终存放结果的目录
	 */
	protected String getParamOutputTargetFilePath(Map<String, String> paraMap) {
		String path = paraMap.get("outputTargetFilePath");
		if (null == path || path.trim().length() <= 0) {
			MRLog.warn(LOG, "MR最终存放结果的目录<outputTargetFilePath>为空");
			return null;
		}
		return path;
	}

	/**
	 * 获取reduce数量(map直接输出，reduce数量必须为0)
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected int getParamNumReduceTasks(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("numReduceTasks");
		int reduceNumber = 0;

		try {
			reduceNumber = Integer.valueOf(para);
		} catch (Exception e) {
			String meg = "reduce数量<numReduceTasks>异常";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		boolean isMapEnd = getParamSysIsMapEnd(paraMap);
		if (isMapEnd && reduceNumber > 0) {
			String meg = "Map直接输出，reduce数量<numReduceTasks>必须为0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (!isMapEnd && reduceNumber <= 0) {
			String meg = "reduce数量<numReduceTasks>必须大于0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return reduceNumber;
	}

	/**
	 * 获取task数量
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected int getParamNumMapTasks(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("numMapTasks");
		int mapNumber = 0;

		try {
			mapNumber = Integer.valueOf(para);
		} catch (Exception e) {
			String meg = "map数量<numMapTasks>异常";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (mapNumber <= 0) {
			String meg = "map数量<numMapTasks>必须大于0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return mapNumber;
	}

	/**
	 * 获取目标文件行分隔符
	 * 
	 * @param paraMap
	 * @return
	 */
	protected String getParamOutputFileRowsSplitChars(Map<String, String> paraMap) {
		String para = paraMap.get("outputFileRowsSplitChars");
		if (null == para || para.length() <= 0) {
			LOG.warn("输出行分隔符<outputFileRowsSplitChars>未设置，默认为'\n'");
			return "\n";
		}
		return para;
	}

	/**
	 * 获取目标文件列分隔符
	 * 
	 * @param paraMap
	 * @return
	 */
	protected String getParamOutputFileFieldSplitChars(Map<String, String> paraMap) {
		String para = paraMap.get("outputFileFieldSplitChars");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "输出行列隔符<outputFileFieldSplitChars>未设置，默认为','");
			return ",";
		}

		return para;
	}

	/**
	 * 获取输出文件格式
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamOutputFileType(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("outputFileType");
		if (null == para || para.length() <= 0) {
			MRLog.warn(LOG, "输出文件类型<outputFileType>未设置，默认为'TEXTFILE'");
			return FileType.TEXTFILE.name();
		}

		if (!(FileType.TEXTFILE.name().equals(para) || FileType.SEQUENCEFILE.name().equals(para) || FileType.RCFILE
				.name().equals(para))) {
			String meg = "输出文件类型<outputFileType>设置错误，可选'TEXTFILE','SEQUENCEFILE','RCFILE'";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;

	}

	/**
	 * 获取目标字段
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String[] getParamOutputFieldNames(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("outputFieldNames");
		if (null == para || para.trim().length() <= 0) {
			String meg = "目标字段<outputFieldNames>为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para.split(","); // 输出字段
	}

	/**
	 * 获取目标字段
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected Set<String> getParamTargetFieldNameList(Map<String, String> paraMap) throws Exception {
		Set<String> setFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setFieldNames, this.getParamOutputFieldNames(paraMap));
		return setFieldNames;
	}

	/**
	 * 获取是否在map中合并
	 * 
	 * @param paraMap
	 * @return
	 */
	protected boolean getParamInputIsCombinerClass(Map<String, String> paraMap) {
		String para = paraMap.get("inputIsCombinerClass");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "是否在map中合并<inputIsCombinerClass>未设置，默认为合并");
			return true;
		}

		return Boolean.valueOf(para);
	}

	/**
	 * 获取目标字段:源字段:统计方法
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamRelatedGroupFieldMethod(Map<String, String> paraMap) throws Exception {
		if (this.getParamSysIsMapEnd(paraMap)) {
			return "";
		}

		String para = paraMap.get("relatedGroupFieldMethod");
		if ((null == para || para.trim().length() <= 0)) {
			String meg = "目标字段:源字段:统计方法<relatedGroupFieldMethod>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 容错处理没有统计方法
		StringBuilder parameter = new StringBuilder();
		String[] fields = para.split(",");
		for (int i = 0; i < fields.length; i++) {
			String per[] = fields[i].split(":");
			switch (per.length) {
			case 2:
				parameter.append(fields[i]);
				parameter.append(":NONE");
				if (i < fields.length - 1) {
					parameter.append(",");
				}
				break;
			case 3:
				parameter.append(fields[i]);
				if (i < fields.length - 1) {
					parameter.append(",");
				}
				break;
			default:
				String meg = "目标字段:源字段:统计方法<relatedGroupFieldMethod>设置错误.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		return parameter.toString();
	}

	/**
	 * 获取源字段与目标字段的对应关系(map直接输出有效)
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamRelatedInToOutField(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("relatedInToOutField");
		if (this.getParamSysIsMapEnd(paraMap) && (null == para || para.trim().length() <= 0)) {
			String meg = "源字段与目标字段的对应关系(map直接输出有效)<relatedInToOutField>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
		return para;
	}

	/**
	 * 获取源文件类型(包括：TEXT,SEQUENCE,RC)
	 * 
	 * @param paraMap
	 * @return 若为空,默认返回TEXT
	 * @throws Exception
	 */
	protected String getParamInputFileType(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputFileType");
		if (null == para || para.length() <= 0) {
			MRLog.warn(LOG, "源文件类型<inputFileType>未设置，默认为'TEXTFILE'");
			return FileType.TEXTFILE.name();
		}

		if (!(FileType.TEXTFILE.name().equals(para) || FileType.SEQUENCEFILE.name().equals(para) || FileType.RCFILE
				.name().equals(para))) {
			String meg = "源文件类型<inputFileType>设置错误，可选'TEXTFILE','SEQUENCEFILE','RCFILE'";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取源文件行分隔符
	 * 
	 * @param paraMap
	 * @return
	 */
	protected String getParamInputFileRowsSplitChars(Map<String, String> paraMap) {
		String para = paraMap.get("inputFileRowsSplitChars");
		if (null == para || para.length() <= 0) {
			MRLog.warn(LOG, "源文件行分隔符<inputFileRowsSplitChars>未设置，默认为'\n'");
			return "\n";
		}
		return para;
	}

	/**
	 * 获取源文件列分隔符
	 * 
	 * @param paraMap
	 * @return
	 */
	protected String getParamInputFileFieldSplitChars(Map<String, String> paraMap) {
		String para = paraMap.get("inputFileFieldSplitChars");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "源文件列分隔符<inputFileFieldSplitChars>未设置，默认为','");
			return ",";
		}
		return para;
	}

	/**
	 * 获取源字段
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamInputFieldNames(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputFieldNames");
		if (null == para || para.trim().length() <= 0) {
			String meg = "源字段<inputFieldNames>为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para; // 输入字段
	}

	/**
	 * 获取源字段
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected Set<String> getParamSrcFieldNameList(Map<String, String> paraMap) throws Exception {
		Set<String> setFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setFieldNames, this.getParamInputFieldNames(paraMap).split(","));
		return setFieldNames;
	}

	/**
	 * 获取是否map直接输出
	 * 
	 * @param paraMap
	 * @return
	 */
	protected boolean getParamSysIsMapEnd(Map<String, String> paraMap) {
		String para = paraMap.get("sysIsMapEnd");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "是否map直接输出<sysIsMapEnd>未设置，默认为否");
			return false;
		}

		return Boolean.valueOf(para);
	}

	/**
	 * 获取过滤类型(true：处理匹配正则表达式的文件，false:不处理匹配正则表达式的文件)
	 * 
	 * @param paraMap
	 * @return
	 */
	protected boolean getParamSysRegFilterType(Map<String, String> paraMap) {
		String para = paraMap.get("sysRegFilterType");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "过滤类型<sysRegFilterType>未设置，默认为false");
			return false;
		}

		return Boolean.valueOf(para);
	}

	/**
	 * 获取过滤的正则表达式
	 * 
	 * @param paraMap
	 * @return
	 */
	protected String getParamSysRegFilterFile(Map<String, String> paraMap) {
		String para = paraMap.get("sysRegFilterFile");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "过滤的正则表达式<sysRegFilterFile>未设置，默认为空，所有文件均不过滤");
			return "";
		}
		return para;
	}

	/**
	 * 获取是否启用文件过滤标识
	 * 
	 * @param paraMap
	 * @return
	 */
	protected boolean getParamSysIsFilter(Map<String, String> paraMap) {
		String para = paraMap.get("sysIsFilter");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "是否启用文件过滤标识<sysIsFilter>未设置，默认为false, 不启用.");
			return false;
		}

		return Boolean.valueOf(para);
	}

	/**
	 * 获取输出路径
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamSysDirOutput(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("sysDirOutput");
		if (null == para || para.trim().length() <= 0) {
			String meg = "输出路径<sysDirOutput>必须设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
		return para;
	}

	/**
	 * 获取输入路径
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamSysDirInput(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("sysDirInput");
		if (null == para || para.trim().length() <= 0) {
			String meg = "输入路径<sysDirInput>必须设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取jobhost
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamSysJobTrackerHost(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("sysJobTrackerHost");
		if (null == para || para.trim().length() <= 0) {
			String meg = "jobhost<sysJobTrackerHost>必须设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取sysHdfsBaseUrl文件系统地址
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamSysHdfsBaseUrl(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("sysHdfsBaseUrl");
		if (null == para || para.trim().length() <= 0) {
			String meg = "文件系统地址<sysHdfsBaseUrl>必须设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取job优先级
	 * 
	 * @param paraMap
	 * @return
	 */
	protected JobPriority getParamSysJobPriority(Map<String, String> paraMap) {
		String para = paraMap.get("sysJobPriority");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "job优先级<sysJobPriority>未设置,默认为NORMAL.");
			return JobPriority.NORMAL;
		}

		return JobPriority.valueOf(para);
	}

	/**
	 * 获取job名称
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String getParamSysJobName(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("sysJobName");
		if (null == para || para.trim().length() <= 0) {
			String meg = "job名称<sysJobName>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取资源文件
	 * 
	 * @param paraMap
	 * @return
	 */
	protected String[] getParamSysResource(Map<String, String> paraMap) {
		String para = paraMap.get("sysResource");
		if (null == para || para.trim().length() <= 0) {
			LOG.warn("资源文件<sysResource>未设置");
			return new String[0];
		}

		return para.split(",");
	}

	/**
	 * 【针对输出到文件设置】：压缩格式 压缩格式：HDFSUtils.CompressCodec
	 * 
	 * @param paraMap
	 * @return
	 */
	protected String getParamOutputCompressCodec(Map<String, String> paraMap) {
		String para = paraMap.get("outputCompressCodec");
		if (null == para || para.trim().length() <= 0) {
			String meg = "[MR ERROR]目标文件压缩方式<outputCompressCodec>未设置, 默认不压缩.";
			LOG.error(meg);
			return null;
		}

		return para;
	}

	/**
	 * 【针对输出到文件设置】：是否压缩,true：表示压缩，否则:不压缩
	 * 
	 * @param paraMap
	 * @return
	 */
	protected boolean getParamOutputCompress(Map<String, String> paraMap) {
		String para = paraMap.get("outputCompress");
		if (null == para || para.trim().length() <= 0) {
			String meg = "[MR ERROR]目标文件是否压缩<outputCompress>未设置, 默认不压缩.";
			LOG.error(meg);
			return false;
		}

		return Boolean.valueOf(para);
	}

	/**
	 * 源数据是数据库表，输入参数验证
	 * 
	 * @param pInputSplitType
	 * @param pInputQuery
	 * @param pInputCountQuery
	 * @param pInputTableName
	 * @param pInputFieldNames
	 * @param pInputConditions
	 * @param pInputBoundingQuery
	 * @param pInputOrderBy
	 * @throws Exception
	 */
	protected void validateInputParameter(int pInputSplitType, String pInputQuery, String pInputCountQuery,
			String pInputTableName, String pInputFieldNames, String pInputConditions, String pInputBoundingQuery,
			String pInputOrderBy) throws Exception {
		boolean isNullpInputQuery = (null == pInputQuery || pInputQuery.trim().length() <= 0);
		boolean isNullpInputCountQuery = (null == pInputCountQuery || pInputCountQuery.trim().length() <= 0);
		boolean isNullpInputTableName = (null == pInputTableName || pInputTableName.trim().length() <= 0);
		boolean isNullpInputFieldNames = (null == pInputFieldNames || pInputFieldNames.trim().length() <= 0);
		boolean isNullpInputConditions = (null == pInputConditions || pInputConditions.trim().length() <= 0);
		boolean isNullpInputBoundingQuery = (null == pInputBoundingQuery || pInputBoundingQuery.trim().length() <= 0);
		boolean isNullpInputOrderByColumn = (null == pInputOrderBy || pInputOrderBy.trim().length() <= 0);

		// 通过拆分类型验证
		String meg = "";
		switch (pInputSplitType) {
		case 1:
			// A.参数设置(设置：inputQuery,inputCountQuery,可选inputFieldNames和inputTableName至少设置一个)
			if (!isNullpInputQuery && !isNullpInputCountQuery) {
				if (!isNullpInputFieldNames) {
					break;
				}

				if (isNullpInputFieldNames && isNullpInputTableName) {
					meg = "源字段<inputFieldNames>未设置的情况下，表名<inputTableName>不能为空.";
					MRLog.error(LOG, meg);
					throw new Exception(meg);
				}

				break;
			}

			// B.参数设置(设置必选：inputQuery,inputTableName,可选:inputFieldNames,inputConditions)
			if (!isNullpInputQuery && !isNullpInputTableName) {
				if (isNullpInputFieldNames) {
					MRLog.warn(LOG, "源字段<inputFieldNames>未设置，默认为<inputQuery>查询参数的字段.");
				}

				if (isNullpInputConditions) {
					MRLog.warn(LOG, "查询条件<inputConditions>未设置,默认为空.");
				}
				break;
			}

			// c.参数设置(设置必选：inputCountQuery,inputTableName,可选:inputFieldNames,inputConditions,inputOrderBy)
			if (!isNullpInputCountQuery && !isNullpInputTableName) {
				if (isNullpInputFieldNames) {
					meg = "源字段<inputFieldNames>未设置，默认为<inputTableName>表的所有字段.";
					MRLog.warn(LOG, meg);
				}

				if (isNullpInputConditions) {
					MRLog.warn(LOG, "查询条件<inputConditions>未设置,默认为空.");
				}

				if (isNullpInputOrderByColumn) {
					MRLog.warn(LOG, "排序条件<inputOrderByColumn>未设置,默认为空.");
				}
				break;
			}

			// d.参数设置(设置：inputTableName,可选:inputFieldNames,inputConditions,inputOrderBy)
			if (!isNullpInputTableName) {
				if (isNullpInputFieldNames) {
					meg = "源字段<inputFieldNames>未设置，默认为<inputTableName>表的所有字段.";
					MRLog.warn(LOG, meg);
				}

				if (isNullpInputConditions) {
					MRLog.warn(LOG, "查询条件<inputConditions>未设置,默认为空.");
				}

				if (isNullpInputOrderByColumn) {
					MRLog.warn(LOG, "排序条件<inputOrderByColumn>未设置,默认为空.");
				}
				break;
			}

			meg = "拆分类型为1的方式，输入参数情况都不满足.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		case 2:
			// A.参数设置(必填：inputQuery, inputBoundingQuery, inputOrderByColumn,
			// 可选：inputTableName,inputFieldNames)
			if (!isNullpInputQuery && !isNullpInputBoundingQuery && !isNullpInputOrderByColumn) {
				if (!(pInputQuery.indexOf("$CONDITIONS") > 0)) {
					meg = "源查询语句<InputQuery>必须包含'$CONDITIONS'内容,否则无法实现数据驱动拆分.";
					MRLog.error(LOG, meg);
					throw new Exception(meg);
				}

				if (isNullpInputTableName) {
					if (isNullpInputFieldNames) {
						meg = "源字段<inputTableName>未设置的情况下，表名<inputFieldNames>不能为空.";
						MRLog.error(LOG, meg);
						throw new Exception(meg);
					}
					MRLog.warn(LOG, "源字段<inputTableName>未设置,默认为空.");
				}

				if (isNullpInputFieldNames) {
					if (isNullpInputTableName) {
						meg = "源字段<inputFieldNames>未设置的情况下，表名<inputTableName>不能为空.";
						MRLog.error(LOG, meg);
						throw new Exception(meg);
					}
					MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为查询语句<inputQuery>的字段.");
				}
			}

			// b.参数设置(必填：inputQuery, inputTableName, inputOrderByColumn,
			// 可选：inputBoundingQuery,inputFieldNames,inputConditions)
			if (!isNullpInputQuery && !isNullpInputOrderByColumn && !isNullpInputTableName) {
				if (isNullpInputFieldNames) {
					MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为查询语句<inputQuery>的字段.");
				}

				if (isNullpInputBoundingQuery) {
					MRLog.warn(LOG, "绑定查询<inputBoundingQuery>未设置,默认通过<inputOrderByColumn>拼接.");
				}

				if (isNullpInputConditions) {
					MRLog.warn(LOG,
							"查询条件<inputConditions>未设置, 默认为空.注：若<inputQuery>有除了$CONDITIONS之外的查询语句，那<inputConditions>必须和其一致");
				}
			}

			// c.参数设置(必填：inputBoundingQuery, inputTableName, inputOrderByColumn,
			// 可选:inputQuery,inputFieldNames,inputConditions)
			if (!isNullpInputBoundingQuery && !isNullpInputTableName && !isNullpInputOrderByColumn) {
				if (isNullpInputQuery) {
					MRLog.warn(LOG, "源查询语句<inputQuery>未设置,为默认值.");
				}

				if (isNullpInputFieldNames) {
					MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为查询语句<inputQuery>的字段.");
				}

				if (isNullpInputConditions) {
					MRLog.warn(LOG,
							"查询条件<inputConditions>未设置, 默认为空.注：若<inputBoundingQuery>含有查询语句，那<inputConditions>必须和其一致");
				}
			}

			// d.参数设置(必填： inputTableName, inputOrderByColumn,
			// 可选:inputQuery,inputBoundingQuery,inputFieldNames,inputConditions)
			if (!isNullpInputTableName && !isNullpInputOrderByColumn) {
				if (isNullpInputQuery) {
					MRLog.warn(LOG, "源查询语句<inputQuery>未设置,为默认值.");
				}

				if (isNullpInputBoundingQuery) {
					MRLog.warn(LOG, "绑定查询<inputBoundingQuery>未设置,默认通过<inputOrderByColumn>拼接.");
				}

				if (isNullpInputFieldNames) {
					MRLog.warn(LOG, "源字段<inputFieldNames>未设置,默认为表的所有字段.");
				}

				if (isNullpInputConditions) {
					MRLog.warn(LOG, "查询条件<inputConditions>未设置, 默认为空.");
				}
			}
			break;
		default:
			break;
		}
	}

	/**
	 * 打印日志
	 * 
	 * @param paraMap
	 */
	public void printParameter(Map<String, String> paraMap) {
		System.out.println("参数配置如下:");
		TreeSet<String> ts = new TreeSet<String>();
		ts.addAll(paraMap.keySet());
		Iterator<String> i = ts.iterator();
		while (i.hasNext()) {
			String key = i.next();
			System.out.println(key + ":" + paraMap.get(key));
		}
	}
}
