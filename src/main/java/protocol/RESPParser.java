package protocol;

import java.util.ArrayList;
import java.util.List;

public class RESPParser {

	public static final byte DOLLAR_BYTE = '$';
	public static final byte ASTERISK_BYTE = '*';
	public static final byte PLUS_BYTE = '+';
	public static final byte MINUS_BYTE = '-';
	public static final byte COLON_BYTE = ':';

	private RESPParser() {
		// this prevent the class from instantiation
	}

	public static List<String> process(String input) {
		if (input.isEmpty()) {
			System.out.println("Error: Empty input!");
			return new ArrayList<>();
		}

		byte type = (byte) input.charAt(0);

		switch (type) {
		case PLUS_BYTE:
			return processSimpleReply(input);
		case ASTERISK_BYTE:
			return processMultiBulkReply(input);
		default:
			throw new IllegalArgumentException("Unknown reply: " + (char) type);
		}
	}

	private static List<String> processMultiBulkReply(String input) {
		List<String> result = new ArrayList<>();
		String[] lines = input.split("\r\n");

		int numeberOfElements = Integer.parseInt(lines[0].substring(1));
		int index = 1; // start after "*<num>" number array elements next element should start with
						// "$<num>" (bulk string)

		for (int i = 0; i < numeberOfElements; i++) {
			if (index + 1 >= lines.length) {
				break;
			}

			if (lines[index].charAt(0) == DOLLAR_BYTE) {
				int length = Integer.parseInt(lines[index].substring(1));
				if (length == -1) {
					// it might be changed to throw error in future
					result.add(null);
				} else {
					String value = lines[index + 1];
					result.add(value);
				}

				index += 2; // go to the next element (skipping length "$<num>)
			}
		}

		return result;
	}

	private static List<String> processSimpleReply(String input) {
		return null;
		// TODO Auto-generated method stub

	}

}
