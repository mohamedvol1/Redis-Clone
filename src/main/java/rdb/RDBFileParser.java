package rdb;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class RDBFileParser {

	private static final int BUFFER_SIZE = 8192;
	private static final String HEADER_STRING = "REDIS0011";
	private static final byte DATABASE_SELECTOR_BYTE = (byte) 0xFE;
	private static final byte METADATA_START_BYTE = (byte) 0xFA;
	private static final byte HASH_TABLE_SIZE_BYTE = (byte) 0xFB;
	private static final byte EX_TIME_SECONDS_BYTE = (byte) 0xFD;
	private static final byte EX_TIME_MILLISECONDS_BYTE = (byte) 0xFC;
	private static final byte FILE_END_BYTE = (byte) 0xFF;
	private static final int STRING_VALUE_TYPE = 0;

	public static Map<String, String> parseFile(String filePath) throws IOException {
		Map<String, String> dataStore = new HashMap<>();
		try (FileInputStream fis = new FileInputStream(filePath)) {
			FileChannel ch = fis.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			ch.read(buffer);
			buffer.flip();

			validateHeader(buffer);
			skipMetaData(buffer);
			parseDataBaseSection(buffer, dataStore);
			verifyFileEnd(buffer);

			return dataStore;
		}
	}

	private static void validateHeader(ByteBuffer buffer) throws IOException {
		byte[] headerBytes = new byte[HEADER_STRING.length()];
		buffer.get(headerBytes);
		String header = new String(headerBytes);
		if (!HEADER_STRING.equals(header)) {
			throw new IOException("Invalid RDB file header: expected " + HEADER_STRING + ", got " + header);
		}
	}

	private static void skipMetaData(ByteBuffer buffer) throws IOException {
		while (buffer.hasRemaining() && buffer.get() != DATABASE_SELECTOR_BYTE) {
			byte previousByte = buffer.get(buffer.position() - 1);
			if (previousByte == METADATA_START_BYTE) {
				skipEncodedString(buffer); // skip key
				skipEncodedString(buffer); // skip value
			} else {
				throw new IOException("Unexpected byte in metadata: " + previousByte);
			}
		}
	}

	private static void parseDataBaseSection(ByteBuffer buffer, Map<String, String> dataStore) throws IOException {
		int index = readEncodedSize(buffer);
		if (index != 0) {
			throw new IOException("Only database 0 is supported, got " + index);
		}

		byte marker = buffer.get();
		if (marker != HASH_TABLE_SIZE_BYTE) {
			throw new IOException("Expected hash table size marker (0xFB), got " + marker);
		}

		int dataSize = readEncodedSize(buffer);
		readEncodedSize(buffer); // skip expiry size

		for (int i = 0; i < dataSize; i++) {
			parseData(buffer, dataStore);
		}
	}

	private static void parseData(ByteBuffer b, Map<String, String> ds) throws IOException {
		byte nextByte = b.get();
		if (nextByte == EX_TIME_SECONDS_BYTE) {
			b.position(b.position() + 4);
		} else if (nextByte == EX_TIME_MILLISECONDS_BYTE) {
			b.position(b.position() + 8);
		} else {
			b.position(b.position() - 1); // no expiry
		}

		int valueType = b.get();
		if (valueType != STRING_VALUE_TYPE) {
			throw new IOException("Only string values supported, got value type " + valueType);
		}

		String key = readEncodedString(b);
		String value = readEncodedString(b);
		ds.put(key, value);
	}

	private static void verifyFileEnd(ByteBuffer b) throws IOException {
		byte endMarker = b.get();
		if (endMarker != FILE_END_BYTE) {
			throw new IOException("Expected file end marker (0xFF), got " + endMarker);
		}
		b.position(b.position() + 8); // skip checksum
	}

	private static int readEncodedSize(ByteBuffer b) throws IOException {
		byte firstByte = b.get();
		int type = (firstByte & 0xC0) >> 6;

		switch (type) {
		case 0:
			return firstByte & 0x3F;
		case 1:
			byte secondByte = b.get();
			return ((firstByte & 0x3F) << 8) | (secondByte & 0xFF);
		case 2:
			return b.getInt();
		default:
			throw new IOException("Invalid size encoding type: " + type);
		}
	}

	private static String readEncodedString(ByteBuffer b) throws IOException {
		int originalPosition = b.position();
		byte firstByte = b.get();
		int encodingType = (firstByte & 0xC0) >> 6;

		if (encodingType == 3) {
			return readSpecialEncodedString(b, firstByte);
		} else {
			b.position(originalPosition);
			int length = readEncodedSize(b);
			byte[] bytes = new byte[length];
			b.get(bytes);
			return new String(bytes);
		}
	}

	private static String readSpecialEncodedString(ByteBuffer b, byte firstByte) throws IOException {
		int format = firstByte & 0x3F;
		switch (format) {
		case 0:
			return String.valueOf(b.get());
		case 1: {
			byte[] bytes = new byte[2];
			b.get(bytes);
			short value = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
			return String.valueOf(value);
		}
		case 2: {
			byte[] bytes = new byte[4];
			b.get(bytes);
			int value = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
			return String.valueOf(value);
		}
		case 3:
			throw new IOException("LZF compression not supported");
		default:
			throw new IOException("Unknown special encoding: 0x" + Integer.toHexString(format));
		}
	}

	private static void skipEncodedString(ByteBuffer b) throws IOException {
		int originalPosition = b.position();
		byte firstByte = b.get();
		int encodingType = (firstByte & 0xC0) >> 6;

		if (encodingType == 3) {
			skipSpecialEncodedString(b, firstByte);
		} else {
			b.position(originalPosition);
			int length = readEncodedSize(b);
			b.position(b.position() + length);
		}
	}

	private static void skipSpecialEncodedString(ByteBuffer b, byte firstByte) throws IOException {
		int format = firstByte & 0x3F;
		switch (format) {
		case 0:
			b.position(b.position() + 1);
			break;
		case 1:
			b.position(b.position() + 2);
			break;
		case 2:
			b.position(b.position() + 4);
			break;
		case 3:
			throw new IOException("LZF compression not supported");
		default:
			throw new IOException("Unknown special encoding type: " + format);
		}
	}
}
