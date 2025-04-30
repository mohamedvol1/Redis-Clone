package protocol;

import java.util.ArrayList;
import java.util.List;

public class RESPParser {

    public static final byte DOLLAR_BYTE = '$';
    public static final byte ASTERISK_BYTE = '*';
    public static final byte PLUS_BYTE = '+';
    public static final byte MINUS_BYTE = '-';
    public static final byte COLON_BYTE = ':';
    private static StringBuilder buffer = new StringBuilder();

    private RESPParser() {
        // this prevent the class from instantiation
    }

    public static List<String> process(String input) {
        System.out.println("\u001B[31mprocess master command input" + input + "\u001B[0m");
        if (input.isEmpty()) {
            System.out.println("Error: Empty input!");
            return new ArrayList<>();
        }

        byte type = (byte) input.charAt(0);
        System.out.println("\u001B[31mprocess master command type" + type + "\u001B[0m");

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
        System.out.println("\u001B[31mprocess master process array input" + input + "\u001B[0m");
        List<String> result = new ArrayList<>();
        String[] lines = input.split("\r\n");
        System.out.println("\u001B[31mprocess master command lines" + lines.toString() + "\u001B[0m");

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

        System.out.println("\u001B[31mprocess master process result" + result.toString() + "\u001B[0m");
        return result;
    }

    public static List<List<String>> processBufferData(String data) {
        buffer.append(data);

        List<List<String>> commands = new ArrayList<>();

        while (!buffer.isEmpty()) {
            try {
                int[] result = findCompleteCommand();
                if (result[0] == -1) {
                    break;
                }

                String commandStr = buffer.substring(0, result[0]);
                // remove processed command from buffer
                buffer.delete(0, result[0]);

                List<String> command = process(commandStr);
                if (!command.isEmpty()) {
                    commands.add(command);
                }
            } catch (Exception e) {
                System.out.println("\u001B[31mError parsing command: " + e.getMessage() + "\u001B[0m");
                // Clear a portion of the buffer to recover from parsing errors
                if (buffer.length() > 0) {
                    // Find next command marker '*' or clear everything if not found
                    int nextCommand = buffer.indexOf("*", 1);
                    if (nextCommand != -1) {
                        buffer.delete(0, nextCommand);
                    } else {
                        buffer.setLength(0);
                    }
                }
            }
        }

        return commands;
    }

    private static int[] findCompleteCommand() {
        if (buffer.length() < 3) {
            return new int[]{-1, 0};
        }

        if (buffer.charAt(0) != (char) ASTERISK_BYTE) {
            return new int[]{-1, 0};
        }

        int newLinePos = buffer.indexOf("\r\n");
        if (newLinePos == -1) {
            return new int[]{-1, 0};
        }

        int arraySize = Integer.parseInt(buffer.substring(1, newLinePos));

        int pos = newLinePos + 2; // moves to start of the next command <$num>

        for (int i = 0; i < arraySize; i++) {
            if (pos + 1 >= buffer.length()) {
                return new int[]{-1, 0};
            }

            if (buffer.charAt(pos) != (char) DOLLAR_BYTE) {
                return new int[]{-1, 0};
            }

            int cmdPartLengthEndPos = buffer.indexOf("\r\n", pos); // $<num>
            if (cmdPartLengthEndPos == -1) {
                return new int[]{-1, 0};
            }

            int cmdPartLength;
            try {
                cmdPartLength = Integer.parseInt(buffer.substring(pos + 1, cmdPartLengthEndPos));
            } catch (NumberFormatException e) {
                return new int[]{-1, 0};
            }

            // Calculate position of the end of this bulk string
            int cmdPartEndPos = cmdPartLengthEndPos + 2 + cmdPartLength + 2;

            if (cmdPartEndPos > buffer.length()) {
                return new int[]{-1, 0};
            }

            pos = cmdPartEndPos;
        }
        return new int[]{pos, arraySize};
    }

    public void clear() {
        buffer.setLength(0);
    }

    private static List<String> processSimpleReply(String input) {
        return null;
        // TODO Auto-generated method stub

    }

}
