package com.ery.hadoop.mrddx.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBMapper;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.FileMapper;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.RCFileMapper;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.RCFileOutputFormat;
import com.ery.hadoop.mrddx.file.RegexPathFilter;
import com.ery.hadoop.mrddx.file.SequenceFileOutputFormat;
import com.ery.hadoop.mrddx.file.TextInputFormat;
import com.ery.hadoop.mrddx.file.TextOutputFormat;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.HDFSUtils.FileType;

/**
 * 从文件到文件的MRService
 * 



 * @createDate 2013-1-15
 * @version v1.0
 */
public class MRFileToFileService extends MRTestJOBService {
	public static final Log LOG = LogFactory.getLog(MRFileToFileService.class);

	/**
	 * 运行job
	 * 
	 * @throws Exception
	 */
	public void run(Map<String, String> paraMap) throws Exception {
		// 获取系统参数
		String pSysResource[] = this.getParamSysResource(paraMap);// 获取资源文件
		String pSysJobName = this.getParamSysJobName(paraMap);// 获取job名称
		JobPriority pSysJobPriority = this.getParamSysJobPriority(paraMap);// 获取job优先级
		String pSysHdfsBaseUrl = this.getParamSysHdfsBaseUrl(paraMap);// 获取hdfsBaseUrl文件系统地址
		String pSysJobTrackerHost = this.getParamSysJobTrackerHost(paraMap);// 获取jobhost
		String pSysDirInput = this.getParamSysDirInput(paraMap);// 获取输入路径
		String pSysDirOutput = this.getParamSysDirOutput(paraMap);// 获取输出路径
		boolean pSysIsFilter = this.getParamSysIsFilter(paraMap);// 获取是否启用文件过滤标识
		String pSysRegFilterFile = this.getParamSysRegFilterFile(paraMap);// 获取过滤的正则表达式
		boolean pSysRegFilterType = this.getParamSysRegFilterType(paraMap);// 获取过滤类型

		// 获取输入参数
		String pInputFileType = this.getParamInputFileType(paraMap);// 获取文件类型
		String pInputFieldNames = this.getParamInputFieldNames(paraMap);// 获取源字段
		String pInputFileFieldSplitChars = this.getParamInputFileFieldSplitChars(paraMap);// 获取源文件列分隔符
		String pInputFileRowsSplitChars = this.getParamInputFileRowsSplitChars(paraMap);// 获取源文件行分隔符
		boolean pInputIsCombinerClass = this.getParamInputIsCombinerClass(paraMap);// 获取是否在map中合并

		// 获取输出参数
		String pOutputFileType = this.getParamOutputFileType(paraMap);// 获取目标字段
		String pOutputFieldNames[] = this.getParamOutputFieldNames(paraMap);// 获取目标字段
		String pOutputFileFieldSplitChars = this.getParamOutputFileFieldSplitChars(paraMap);// 获取目标文件列分隔符
		String pOutputFileRowsSplitChars = this.getParamOutputFileRowsSplitChars(paraMap);// 获取目标文件行分隔符
		boolean pOutputCompress = this.getParamOutputCompress(paraMap);// 获取目标文件是否压缩
		String pOutputCompressCodec = this.getParamOutputCompressCodec(paraMap);// 获取目标文件压缩方式
		int pNumMapTasks = this.getParamNumMapTasks(paraMap);// 获取task数量
		int pNumReduceTasks = this.getParamNumReduceTasks(paraMap);// 获取reduce数量(map直接输出，reduce数量必须为0)
		String pOutputFilePath = this.getParamOutputTargetFilePath(paraMap);// 获取MR最终存放结果的目录

		// 获取输出输出关系参数
		String pRelatedGroupFieldMethod = this.getParamRelatedGroupFieldMethod(paraMap);// 获取目标字段:源字段:统计方法

		// 打印解析后的输出参数参数
		this.printParameter(paraMap);

		try {
			Configuration conf = new Configuration();

			String[] dirInputs = pSysDirInput.split(",");
			Path inPaths[] = new Path[dirInputs.length];
			for (int i = 0; i < dirInputs.length; i++) {
				inPaths[i] = new Path(dirInputs[i]);
			}
			FileSystem fs = FileSystem.get(conf);
			Path outPath = new Path(pSysDirOutput);
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}

			for (int i = 0; i < pSysResource.length; i++) {
				conf.addResource(pSysResource[i]);
			}
			Job job = Job.getInstance(conf);
			conf = job.getConfiguration();
			job.setJarByClass(MRFileToFileService.class);
			job.setJobName(pSysJobName);
			job.setPriority(pSysJobPriority);
			conf.set(MRConfiguration.FS_DEFAULT_NAME, pSysHdfsBaseUrl);
			conf.set(MRConfiguration.MAPRED_JOB_TRACKER, pSysJobTrackerHost);
			if (pSysIsFilter) {
				FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
				conf.set(FileConfiguration.INPUT_FILE_REGEXPATHFILTER, pSysRegFilterFile);
				conf.setBoolean(FileConfiguration.INPUT_FILE_REGEXPATHFILTER_TYPE, pSysRegFilterType);
			}

			FileInputFormat.setInputPaths(job, inPaths);
			FileOutputFormat.setOutputPath(job, outPath);
			if (FileType.RCFILE.name().equals(pInputFileType)) {
				job.setMapperClass(RCFileMapper.class);
			} else if (FileType.TEXTFILE.name().equals(pInputFileType)
					|| FileType.SEQUENCEFILE.name().equals(pInputFileType)) {
				job.setMapperClass(FileMapper.class);
			} else {
				job.setMapperClass(DBMapper.class);
			}
			job.setReducerClass(DBReducer.class);

			// 输入配置
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(DBRecord.class);
			job.setMapOutputValueClass(DBRecord.class);
			FileConfiguration dbconf = new FileConfiguration(conf);
			dbconf.setInputFieldNames(pInputFieldNames);// 设置输入字段顺序,与文件中顺序对应
			dbconf.setInputFileFieldSplitChars(pInputFileFieldSplitChars);
			dbconf.setInputFileRowsSplitChars(pInputFileRowsSplitChars);
			dbconf.setRelatedGroupFieldMethod(pRelatedGroupFieldMethod);
			if (pInputIsCombinerClass) {
				job.setCombinerClass(DBGroupReducer.class);
			}

			// 输出配置
			dbconf.setOutputFieldNames(pOutputFieldNames);// 设置输出字段顺序,与文件中顺序对应
			if (FileType.TEXTFILE.name().equals(pOutputFileType)) {
				job.setOutputKeyClass(DBRecord.class);
				job.setOutputValueClass(NullWritable.class);
				job.setOutputFormatClass(TextOutputFormat.class);// 输出文件
				TextOutputFormat.setOutputParameter(job, pOutputCompress, pOutputCompressCodec,
						pOutputFileFieldSplitChars, pOutputFileRowsSplitChars);
			} else if (FileType.SEQUENCEFILE.name().equals(pOutputFileType)) {
				job.setOutputFormatClass(SequenceFileOutputFormat.class);// 输出文件
				SequenceFileOutputFormat.setOutputParameter(job, pOutputCompress, pOutputCompressCodec,
						pOutputFileFieldSplitChars, pOutputFileRowsSplitChars);
			} else if (FileType.RCFILE.name().equals(pOutputFileType)) {
				job.setOutputFormatClass(RCFileOutputFormat.class);// 输出文件
				RCFileOutputFormat.setColumnNumber(conf, pOutputFieldNames.length);
				RCFileOutputFormat.setOutputParameter(conf, pOutputCompress, pOutputCompressCodec,
						pOutputFileFieldSplitChars, pOutputFileRowsSplitChars);
			}
			new MRConfiguration(conf).setNumMapTasks(pNumMapTasks);
			job.setNumReduceTasks(pNumReduceTasks);
			job.waitForCompletion(true);
			// JobClient.runJob(conf);

			// 将文件从临时目录拷贝到目标目录下
			if (null != pOutputFilePath) {
				HDFSUtils.mvFiles(conf, pSysDirOutput + "/part-*", pOutputFilePath);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 例如：业务场景一：只有map
	 * 
	 * @return
	 */
	public static Map<String, String> testNoReduceParamter() {
		// 系统参数
		String sysResource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String sysJobName = "M-R File to File";
		String sysJobPriority = "HIGH";
		String sysHdfsBaseUrl = "hdfs://hadoop01:9000";
		String sysJobTrackerHost = "hadoop01:9001";
		String sysDirInput = "/wanghao/isomerism-result001/";
		String sysDirOutput = "/wanghao/isomerism-result";
		String sysIsFilter = "true"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String sysRegFilterFile = "part-00000";// 过滤正则表达式
		String sysRegFilterType = "true";
		String sysIsInputMapEnd = "true"; // 需要map直接输出

		String inputFileType = "RCFILE"; // 输出文件格式(TEXTFILE,SEQUENCEFILE,RCFILE)
		String inputFieldNames = "sid,sprice,mprice,avgnumber,sumnumber,rowcounts"; // 输入字段
		String inputFileFieldSplitChars = "\\^";
		String inputFileRowsSplitChars = "\n";
		// String inputIsCombinerClass = "false";

		String outputFileType = "TEXTFILE"; // 输出文件格式(TEXTFILE,SEQUENCEFILE,RCFILE)
		String outputFieldNames = "sid,sprice,mprice,avgnumber,sumnumber,rowcounts";// 输出字段
		String outputFileFieldSplitChars = ",";
		String outputFileRowsSplitChars = "\n";
		String outputCompress = "false";
		String outputCompressCodec = HDFSUtils.CompressCodec.SnappyCodec.toString();
		String outputTargetFilePath = "/wanghao/isomerism-result002";

		// 设置任务数
		String numMapTasks = "2";
		// String numReduceTasks = "0";

		// 输入与输出关系
		String relatedInToOutField = "sid:sid,sprice:sprice,mprice:mprice,avgnumber:avgnumber,sumnumber:sumnumber,rowcounts:rowcounts";// 目标字段:源字段

		Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("sysResource", sysResource);
		paraMap.put("sysJobName", sysJobName);
		paraMap.put("sysJobPriority", sysJobPriority);
		paraMap.put("sysHdfsBaseUrl", sysHdfsBaseUrl);
		paraMap.put("sysJobTrackerHost", sysJobTrackerHost);
		paraMap.put("sysDirInput", sysDirInput);
		paraMap.put("sysDirOutput", sysDirOutput);
		paraMap.put("sysIsFilter", sysIsFilter);
		paraMap.put("sysRegFilterFile", sysRegFilterFile);
		paraMap.put("sysRegFilterType", sysRegFilterType);
		paraMap.put("sysIsInputMapEnd", sysIsInputMapEnd);

		paraMap.put("inputFileType", inputFileType);
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("inputIsCombinerClass", "false");

		paraMap.put("outputFileType", outputFileType);
		paraMap.put("outputFieldNames", outputFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);

		paraMap.put("outputCompress", outputCompress);
		paraMap.put("outputCompressCodec", outputCompressCodec);
		paraMap.put("outputTargetFilePath", outputTargetFilePath);

		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", "0");

		paraMap.put("relatedInToOutField", relatedInToOutField);
		return paraMap;
	}

	/**
	 * 例如：业务场景二
	 * 
	 * @return
	 */
	public static Map<String, String> testMRParamter() {
		// 系统参数
		String sysResource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String sysJobName = "M-R File to File";
		String sysJobPriority = "HIGH";
		String sysHdfsBaseUrl = "hdfs://hadoop01:9000";
		String sysJobTrackerHost = "hadoop01:9001";
		String sysDirInput = "/wanghao/isomerism";
		String sysDirOutput = "/wanghao/isomerism-result";
		String sysIsFilter = "true"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String sysRegFilterFile = "mrdata.txt";// 过滤正则表达式
		String sysRegFilterType = "true";

		// 输入参数
		String inputIsMapEnd = "false"; // 需要map直接输出
		String inputFileType = "RCFILE"; // 输出文件格式(TEXTFILE,SEQUENCEFILE,RCFILE)
		String inputFieldNames = "time,id,type,price,number,message"; // 输入字段
		String inputFileFieldSplitChars = "\\^";
		String inputFileRowsSplitChars = "\n";
		String inputIsCombinerClass = "true";

		// 输出参数
		String outputFileType = "RCFILE"; // 输出文件格式(TEXTFILE,SEQUENCEFILE,RCFILE)
		String outputFieldNames = "sid,sprice,mprice,avgnumber,sumnumber,rowcounts";// 输出字段
		String outputFileFieldSplitChars = "^";
		String outputFileRowsSplitChars = "\n";
		String outputCompress = "false";
		String outputCompressCodec = HDFSUtils.CompressCodec.SnappyCodec.toString();
		String outputTargetFilePath = "/wanghao/isomerism-result001";

		// 设置任务数
		String numMapTasks = "5";
		String numReduceTasks = "1";

		// 输入与输出关系
		String relatedGroupFieldMethod = "sid:id:NONE,sprice:price:MIN,mprice:price:MAX,avgnumber:number:AVG,sumnumber:number:SUM,rowcounts:id:COUNT";

		Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("sysResource", sysResource);
		paraMap.put("sysJobName", sysJobName);
		paraMap.put("sysJobPriority", sysJobPriority);
		paraMap.put("sysHdfsBaseUrl", sysHdfsBaseUrl);
		paraMap.put("sysJobTrackerHost", sysJobTrackerHost);
		paraMap.put("sysDirInput", sysDirInput);
		paraMap.put("sysDirOutput", sysDirOutput);
		paraMap.put("sysIsFilter", sysIsFilter);
		paraMap.put("sysRegFilterFile", sysRegFilterFile);
		paraMap.put("sysRegFilterType", sysRegFilterType);

		// 设置输入
		paraMap.put("inputIsMapEnd", inputIsMapEnd);
		paraMap.put("inputFileType", inputFileType);
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("inputIsCombinerClass", inputIsCombinerClass);

		// 设置输出
		paraMap.put("outputFileType", outputFileType);
		paraMap.put("outputFieldNames", outputFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);
		paraMap.put("outputCompress", outputCompress);
		paraMap.put("outputCompressCodec", outputCompressCodec);
		paraMap.put("outputTargetFilePath", outputTargetFilePath);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("relatedGroupFieldMethod", relatedGroupFieldMethod);
		return paraMap;
	}

	/**
	 * 入口函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		MRFileToFileService mr = new MRFileToFileService();
		Map<String, String> param = testNoReduceParamter();
		// Map<String, String> param = testMRParamter();
		mr.run(param);
	}
}