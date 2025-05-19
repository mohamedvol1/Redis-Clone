package command;

import store.DataStore;
import store.Entry;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class IncermentCommand implements Command {
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() != 2) {
            throw new Exception("ERR wrong number of arguments for 'INCR' command");
        }

        String key = commandParts.get(1);

        if (!store.exists(key)) {
            store.set(key, "1");
            String response = ":1\r\n";
            client.write(ByteBuffer.wrap(response.getBytes()));
            return;
        }

        String value = (String) store.get(key);
        Integer IncrementedValue = Integer.parseInt(value) + 1;
        store.set(key, IncrementedValue.toString());
        String response = ":" + IncrementedValue + "\r\n";

        client.write(ByteBuffer.wrap(response.getBytes()));
    }

    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'INCR' command");
    }

    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'INCR' command");
    }
}
