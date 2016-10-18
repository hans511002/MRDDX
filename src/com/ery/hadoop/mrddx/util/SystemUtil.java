package com.ery.hadoop.mrddx.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SystemUtil {

	/**
	 * 获取错误流的信息
	 * 
	 * @param command
	 */
	public static void exece(String command) {
		if (null == command || command.trim().length() <= 0) {
			return;
		}
		try {
			Process proc = Runtime.getRuntime().exec(command);
			exece(proc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取错误流的信息
	 * 
	 * @param command
	 */
	public static void exece(Process proc) {
		if (null == proc) {
			return;
		}

		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			String text = null;
			while ((text = in.readLine()) != null) {
				if (text == null || text.trim().equals("")) {
					continue;
				}
				System.out.println(text);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != in) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 获取流的信息
	 * 
	 * @param command
	 */
	public static void exec(String command) {
		if (null == command || command.trim().length() <= 0) {
			return;
		}

		try {
			Process proc = Runtime.getRuntime().exec(command);
			exec(proc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取流的信息
	 * 
	 * @param command
	 */
	public static void exec(Process proc) {
		if (null == proc) {
			return;
		}

		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String text = null;
			while ((text = in.readLine()) != null) {
				if (text == null || text.trim().equals("")) {
					continue;
				}
				System.out.println(text);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != in) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 获取流的信息
	 * 
	 * @param command
	 */
	public static String execResult(String... command) throws IOException {
		if (null == command || command.length <= 0) {
			return null;
		}
		String text = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(command);
			Process proc = pb.start();
			text = getShellOut(proc);
			proc.destroy();
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
		}
		return text;
	}

	/**
	 * 读取输出流数据
	 * 
	 * @param p
	 *            进程
	 * @return 从输出流中读取的数据
	 * @throws IOException
	 */
	public static final String getShellOut(Process p) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedInputStream in = null;
		BufferedReader br = null;
		try {
			in = new BufferedInputStream(p.getInputStream());
			br = new BufferedReader(new InputStreamReader(in));
			String s;
			while ((s = br.readLine()) != null) {// 追加换行符
				sb.append("\n");
				sb.append(s);
			}
		} catch (IOException e) {
			throw e;
		} finally {
			br.close();
			in.close();
		}
		return sb.toString();
	}

}
