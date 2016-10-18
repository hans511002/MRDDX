/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.ery.hadoop.mrddx.vertica;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.util.StringUtils;

public class Relation {
	private String table = null;
	private String schema = null;
	private String database = null;
	private boolean defSchema = false;

	public Relation(String name) {
		if (name == null)
			return;

		String[] splut = name.split("\\.");

		if (splut.length == 3) {
			database = splut[0];
			schema = splut[1];
			table = splut[2];
		} else if (splut.length == 2) {
			schema = splut[0];
			table = splut[1];
		} else if (splut.length == 1) {
			defSchema = true;
			schema = "public";
			table = splut[0];
		}
	}

	public boolean isNull() {
		return table == null;
	}

	public boolean isDefaultSchema() {
		return defSchema;
	}

	public String getDatabase() {
		return database;
	}

	public String getSchema() {
		return schema;
	}

	public String getTable() {
		return table;
	}

	public StringBuilder getQualifiedName() {
		StringBuilder sb = new StringBuilder();
		if (database != null) {
			sb.append(database);
			sb.append('.');
		}

		sb.append(schema);
		sb.append('.');
		sb.append(table);

		return sb;
	}

	public static int getType(Object obj) {
		if (obj == null) {
			return Types.NULL;
		} else if (obj instanceof Long) {
			return Types.BIGINT;
		} else if (obj instanceof LongWritable) {
			return Types.BIGINT;
		} else if (obj instanceof VLongWritable) {
			return Types.BIGINT;
		} else if (obj instanceof VIntWritable) {
			return Types.INTEGER;
		} else if (obj instanceof Integer) {
			return Types.INTEGER;
		} else if (obj instanceof Short) {
			return Types.SMALLINT;
		} else if (obj instanceof BigDecimal) {
			return Types.NUMERIC;
		} else if (obj instanceof DoubleWritable) {
			return Types.DOUBLE;
		} else if (obj instanceof Double) {
			return Types.DOUBLE;
		} else if (obj instanceof Float) {
			return Types.FLOAT;
		} else if (obj instanceof FloatWritable) {
			return Types.FLOAT;
		} else if (obj instanceof byte[]) {
			return Types.BINARY;
		} else if (obj instanceof ByteWritable) {
			return Types.BINARY;
		} else if (obj instanceof Boolean) {
			return Types.BOOLEAN;
		} else if (obj instanceof BooleanWritable) {
			return Types.BOOLEAN;
		} else if (obj instanceof Character) {
			return Types.CHAR;
		} else if (obj instanceof String) {
			return Types.VARCHAR;
		} else if (obj instanceof BytesWritable) {
			return Types.VARCHAR;
		} else if (obj instanceof Text) {
			return Types.VARCHAR;
		} else if (obj instanceof java.util.Date) {
			return Types.DATE;
		} else if (obj instanceof Date) {
			return Types.DATE;
		} else if (obj instanceof Time) {
			return Types.TIME;
		} else if (obj instanceof Timestamp) {
			return Types.TIMESTAMP;
		} else {
			throw new RuntimeException("Unknown type " + obj.getClass().getName() + " passed to Vertica Record");
		}
	}

	public static void write(Object obj, int type, DataOutput out) throws IOException {
		if (obj == null)
			return;
		switch (type) {
		case Types.BIGINT:
			out.writeLong((Long) obj);
			break;
		case Types.INTEGER:
			out.writeInt((Integer) obj);
			break;
		case Types.TINYINT:
		case Types.SMALLINT:
			out.writeShort((Short) obj);
			break;
		case Types.REAL:
		case Types.DECIMAL:
		case Types.NUMERIC:
			Text.writeString(out, obj.toString());
			break;
		case Types.DOUBLE:
			out.writeDouble((Double) obj);
			break;
		case Types.FLOAT:
			out.writeFloat((Float) obj);
			break;
		case Types.BINARY:
		case Types.LONGVARBINARY:
		case Types.VARBINARY:
			Text.writeString(out, StringUtils.byteToHexString((byte[]) obj));
			break;
		case Types.BIT:
		case Types.BOOLEAN:
			out.writeBoolean((Boolean) obj);
			break;
		case Types.CHAR:
			out.writeChar((Character) obj);
			break;
		case Types.LONGNVARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.VARCHAR:
			Text.writeString(out, (String) obj);
			break;
		case Types.DATE:
			if (obj instanceof java.util.Date) {
				out.writeLong(((java.util.Date) obj).getTime());
			} else {
				out.writeLong(((Date) obj).getTime());
			}
			break;
		case Types.TIME:
			out.writeLong(((Time) obj).getTime());
			break;
		case Types.TIMESTAMP:
			out.writeLong(((Timestamp) obj).getTime());
			break;
		default:
			throw new IOException("Unknown type value " + type);
		}
	}

	public static Object readField(int type, DataInput in) throws IOException {
		if (type == Types.NULL)
			return null;
		else if (type == Types.BIGINT)
			return in.readLong();
		else if (type == Types.INTEGER)
			return in.readInt();
		else if (type == Types.TINYINT || type == Types.SMALLINT)
			return in.readShort();
		else if (type == Types.REAL || type == Types.DECIMAL || type == Types.NUMERIC)
			return new BigDecimal(Text.readString(in));
		else if (type == Types.DOUBLE)
			return in.readDouble();
		else if (type == Types.FLOAT)
			return in.readFloat();
		else if (type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY)
			return StringUtils.hexStringToByte(Text.readString(in));
		else if (type == Types.BIT || type == Types.BOOLEAN)
			return in.readBoolean();
		else if (type == Types.CHAR)
			return in.readChar();
		else if (type == Types.LONGNVARCHAR || type == Types.LONGVARCHAR || type == Types.NCHAR
				|| type == Types.NVARCHAR || type == Types.VARCHAR)
			return Text.readString(in);
		else if (type == Types.DATE)
			return new Date(in.readLong());
		else if (type == Types.TIME)
			return new Time(in.readLong());
		else if (type == Types.TIMESTAMP)
			return new Timestamp(in.readLong());
		else
			throw new IOException("Unknown type value " + type);
	}

}
