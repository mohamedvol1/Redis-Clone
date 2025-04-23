package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class ReplconfCommand implements Command {
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        execute(client, commandParts);
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        try {
            // REPLCONF command always returns OK (to be changed in the future)
            client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing REPLCONF: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        execute(client, null);
    }
}