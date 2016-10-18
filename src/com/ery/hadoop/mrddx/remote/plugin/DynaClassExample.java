package com.ery.hadoop.mrddx.remote.plugin;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.tools.ant.filters.StringInputStream;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.file.LineReaders;
//import org.w3c.dom.Document;
//import org.w3c.dom.Element;
//import org.w3c.dom.Node;
//import org.w3c.dom.NodeList;
//import org.xml.sax.SAXException;
//import org.dom4j.Document;
//import org.dom4j.DocumentException;
//import org.dom4j.DocumentHelper;
//import org.dom4j.Element;
//import org.dom4j.io.SAXReader;
//import org.dom4j.io.XMLWriter;
//import org.jdom.Document;
//import org.jdom.Element;
//import org.jdom.JDOMException;
//import org.jdom.input.SAXBuilder;
//import org.jdom.output.XMLOutputter;
//import org.w3c.dom.NodeList;

public class DynaClassExample extends IRemotePlugin {
	int rowNum = 0;
	StringBuffer sb = new StringBuffer();
	StringBuffer resStr = new StringBuffer();
	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	String fileFormatVersion = "";
	String vendorName = "";
	String elementType = "";
	String beginTime = "";
	static java.util.regex.Pattern datep = Pattern
			.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})T(\\d{1,2}):(\\d{1,2}):(\\d{1,2})");// 2015-04-17T02:00:00+08:00

	String parseDateTime(String dtime) {
		Matcher m = datep.matcher(dtime);
		m.find();
		String yyyy = m.group(1);
		String mm = m.group(2);
		String dd = m.group(3);
		String hh = m.group(4);
		String mi = m.group(5);
		String ss = m.group(6);
		dtime = yyyy + "-" + mm + "-" + dd + " " + hh + ":" + mi + ":" + ss;
		// 可能需要加时区
		return dtime;
	}

	@Override
	public String line(String lineValue) throws IOException {
		boolean needParse = false;
		int nodeType = 0;
		lineValue = lineValue.trim();
		if (lineValue.startsWith("<measCollecFile>")) {// 新文件开始
			sb.setLength(0);
			return null;
		} else if (lineValue.startsWith("<fileHeader")) {// 文件head
			sb.setLength(0);
			sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		} else if (lineValue.endsWith("</fileHeader>")) {// 文件head
			needParse = true;
			nodeType = 1;
		} else if (lineValue.startsWith("<measData")) {
			sb.setLength(0);
			sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		} else if (lineValue.endsWith("</measData>")) {
			needParse = true;
			nodeType = 2;
		} else if (lineValue.startsWith("<fileFooter")) {
			sb.setLength(0);
			sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		} else if (lineValue.endsWith("</fileFooter>")) {
			needParse = true;
			nodeType = 3;
		} else if (lineValue.endsWith("</measCollecFile>")) {// 文件结束
			return null;
		}
		sb.append(lineValue);
		sb.append((char) 10);
		if (!needParse) {
			return null;
		}
		try {
			StringInputStream sin = new StringInputStream(sb.toString());
			// dom
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document document = db.parse(sin);
			NodeList nls = null;// = document.getChildNodes();
			switch (nodeType) {
			case 1:
				nls = document.getElementsByTagName("fileHeader");
				for (int i = 0; i < nls.getLength(); i++) {
					org.w3c.dom.Node node = nls.item(i);
					if (!node.getNodeName().equals("fileHeader"))
						continue;
					fileFormatVersion = node.getAttributes().getNamedItem("fileFormatVersion").getNodeValue();
					vendorName = node.getAttributes().getNamedItem("vendorName").getNodeValue();
					nls = node.getChildNodes();
					for (int j = 0; j < nls.getLength(); j++) {
						node = nls.item(j);
						if (node.getNodeName().equals("fileSender")) {
							elementType = node.getAttributes().getNamedItem("elementType").getNodeValue();
						} else if (node.getNodeName().equals("measCollec")) {
							beginTime = node.getAttributes().getNamedItem("beginTime").getNodeValue();// 2015-04-17T02:00:00+08:00
							beginTime = parseDateTime(beginTime);
						}
					}
					break;
				}
				break;
			case 2:
				resStr.setLength(0);
				nls = document.getElementsByTagName("measData");
				org.w3c.dom.Node node = null;
				for (int i = 0; i < nls.getLength(); i++) {
					node = nls.item(0);
					if (node.getNodeName().equals("measData"))
						break;
				}
				if (!node.getNodeName().equals("measData"))
					break;
				nls = node.getChildNodes();
				String userLabel = "";
				String Server = "";// Server=102900,SBNID=510101,ENODEBID=634884
				String SBNID = "";
				String ENODEBID = "";
				String endTime = "";
				String duration = "";
				for (int j = 0; j < nls.getLength(); j++) {
					node = nls.item(j);
					if (node.getNodeName().equals("managedElement")) {
						String tmp = node.getAttributes().getNamedItem("localDn").getNodeValue();
						String tps[] = tmp.split(",");
						for (int i = 0; i < tps.length; i++) {
							String ts[] = tps[i].split("=");
							if (ts[0].equals("Server")) {
								Server = ts[1];
							} else if (ts[0].equals("SBNID")) {
								SBNID = ts[1];
							} else if (ts[0].equals("ENODEBID")) {
								ENODEBID = ts[1];
							}
						}
						userLabel = node.getAttributes().getNamedItem("userLabel").getNodeValue();
					} else if (node.getNodeName().equals("measInfo")) {
						nls = node.getChildNodes();
						String[] measTypes = null;
						String[] rowValue = null;

						for (int i = 0; i < nls.getLength(); i++) {
							node = nls.item(i);
							if (node.getNodeName().equals("granPeriod")) {
								duration = node.getAttributes().getNamedItem("duration").getNodeValue();
								String tmp = node.getAttributes().getNamedItem("endTime").getNodeValue();
								endTime = parseDateTime(tmp);

							} else if (node.getNodeName().equals("measTypes")) {
								measTypes = node.getTextContent().split(" ");
							} else if (node.getNodeName().equals("measValue")) {
								String tmp = node.getAttributes().getNamedItem("measObjLdn").getNodeValue();
								String tps[] = tmp.split(",");
								String CellID = "";
								for (int k = 0; k < tps.length; k++) {
									String ts[] = tps[k].split("=");
									if (ts[0].equals("CellID")) {
										CellID = ts[1];
										// }else if (ts[0].equals("Server")) {
										// Server = ts[1];
										// } else if (ts[0].equals("SBNID")) {
										// SBNID = ts[1];
										// } else if (ts[0].equals("ENODEBID"))
										// {
										// ENODEBID = ts[1];
									}
								}
								NodeList mrs = node.getChildNodes();
								String suspect = "";
								for (int g = 0; g < mrs.getLength(); g++) {
									node = mrs.item(g);
									if (node.getNodeName().equals("measResults")) {
										String row = node.getTextContent();
										rowValue = row.split(" ");
									} else if (node.getNodeName().equals("suspect")) {
										suspect = node.getTextContent();
									}
								}
								// //////////////////////////合成记录
								for (int k = 0; k < measTypes.length; k++) {
									if (resStr.length() > 0) {
										resStr.append((char) 10);
									}
									resStr.append(fileFormatVersion);
									resStr.append(",");
									resStr.append(vendorName);
									resStr.append(",");
									resStr.append(elementType);
									resStr.append(",");
									resStr.append(beginTime);
									resStr.append(",");
									resStr.append(userLabel);
									resStr.append(",");
									resStr.append(Server);
									resStr.append(",");
									resStr.append(SBNID);
									resStr.append(",");
									resStr.append(ENODEBID);
									resStr.append(",");
									resStr.append(endTime);
									resStr.append(",");
									resStr.append(duration);
									resStr.append(",");
									resStr.append(CellID);
									resStr.append(",");
									resStr.append(suspect);
									resStr.append(",");
									resStr.append(measTypes[k]);
									resStr.append(",");
									resStr.append(rowValue[k]);
									// 请直接生成PUT写入Hbase，横表，不再转为纵表记录输出
								}
							}
						}
					}
				}
				return resStr.toString();
			case 3:
				break;
			}
			// // jdom
			// SAXBuilder builder = new SAXBuilder(false);
			// org.jdom.Document document2 = builder.build(sin);
			// org.jdom.Element employees1 = document2.getRootElement();
			// List employeeList = employees1.getChildren("employee");
			// switch (nodeType) {
			// case 1:
			// break;
			// case 2:
			// break;
			// case 3:
			// break;
			// }
		} catch (ParserConfigurationException | SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Map<String, Object>> recode(Map lineValue) throws IOException {
		return super.recode(lineValue);
	}

	@Override
	public void configure(TaskInputOutputContext job) {
		System.out.println("Hhello, config");
	}

	@Override
	public void close() throws IOException {
		System.out.println("Hello, close");
	}

	public void beforConvertRow(DBRecord row) {

	}

	public String toString() {
		return "Hello, I am " + this.getClass().getSimpleName();
	}

	public static void main(String[] args) throws IOException {
		DynaClassExample dc = new DynaClassExample();

		String file = "102900_LTEFDD_PM_CT_20150417_0200_new.zip";
		FileInputStream fileIn = new FileInputStream(file);
		System.out.println("use ZipInputStream read file " + file);
		ZipInputStream zin = new ZipInputStream(fileIn, Charset.forName("gbk"));
		LineReaders in = new LineReaders(zin, 0);
		Text str = new Text();
		int rownum = 0;
		while (in.readLine(str) > 0) {
			dc.line(str.toString());
			rownum++;
			if (rownum % 10000 == 0) {
				System.out.println(new Date().toLocaleString() + " " + rownum);
			}
		}
		System.out.println(new Date().toLocaleString() + " " + rownum);
	}
}
