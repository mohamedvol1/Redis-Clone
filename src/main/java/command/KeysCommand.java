package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class KeysCommand implements Command {
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        try {
            if (commandParts.size() != 2) {
                throw new Exception("ERR wrong number of arguments for 'KEYS' command");
            }

            if ("*".equals(commandParts.get(1))) {
                List<String> keys = store.getAllKeys();
                StringBuilder response = new StringBuilder();
                response.append("*").append(keys.size()).append("\r\n");
                for (String k : keys) {
                    response.append("$").append(k.length()).append("\r\n").append(k).append("\r\n");
                }
                client.write(ByteBuffer.wrap(response.toString().getBytes()));
            } else {
                throw new Exception("ERR only '*' pattern is supported for 'KEYS' command");
            }
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing KEYS: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR KEYS command requires a data store");
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'KEYS' command");
    }
}