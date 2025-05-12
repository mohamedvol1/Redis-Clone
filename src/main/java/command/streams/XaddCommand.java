package command.streams;

import command.Command;
import store.DataStore;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XaddCommand implements Command {
    @Override
    public void execute (SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() < 4 || (commandParts.size() - 3) % 2 != 0) {
            throw new Exception("ERR wrong number of arguments for 'XADD' command");
        }

        String key = commandParts.get(1);
        String id = commandParts.get(2);

        Map<String, String> fields = new HashMap<>();
        for (int i = 3; i < commandParts.size(); i += 2) {
            fields.put(commandParts.get(i), commandParts.get(i + 1));
        }

        // this to be modified to let method add entries to without specifying id (use overloading)
        String streamEntryId = store.addToStream(key, id, fields);
        String response = "$" + streamEntryId.length() + "\r\n" + streamEntryId + "\r\n";
        client.write(ByteBuffer.wrap(response.getBytes()));
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong context for 'XADD' command");
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong context for 'XADD' command");
    }

}
