package command.streams;

import command.Command;
import store.DataStore;
import store.DataType;
import streams.Stream;
import streams.StreamEntry;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

public class XrangeCommand implements Command {
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() != 4) {
            throw new Exception("ERR wrong number of arguments for 'XRANGE' command");
        }

        String key = commandParts.get(1);
        String startId = commandParts.get(2);
        String endId = commandParts.get(3);

        if (!store.exists(key)) {
            // send empty RESP array
            client.write(ByteBuffer.wrap("*0\r\n".getBytes()));
            return;
        }

        if (store.getDataType(key) != DataType.STREAM) {
            throw new Exception("WRONGTYPE Operation against a key holding the wrong kind of value");
        }


        Stream stream = (Stream) store.get(key);
        List<StreamEntry> entries = stream.getEntriesInRange(startId, endId);

        if (entries.isEmpty()) {
            client.write(ByteBuffer.wrap("*0\r\n".getBytes()));
        }

        // Format response as a RESP Array
        StringBuilder response = new StringBuilder();
        response.append("*").append(entries.size()).append("\r\n");

        for (StreamEntry entry : entries) {
            // Add entry ID and field count
            response.append("*2\r\n");
            response.append("$").append(entry.id().length()).append("\r\n").append(entry.id()).append("\r\n");

            // Add fields array
            Map<String, String> fields = entry.fields();
            response.append("*").append(fields.size() * 2).append("\r\n");

            for (Map.Entry<String, String> field : fields.entrySet()) {
                response.append("$").append(field.getKey().length()).append("\r\n").append(field.getKey()).append("\r\n");
                response.append("$").append(field.getValue().length()).append("\r\n").append(field.getValue()).append("\r\n");
            }
        }

        client.write(ByteBuffer.wrap(response.toString().getBytes()));
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong context for 'XRANGE' command");
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong context for 'XRANGE' command");
    }

}
