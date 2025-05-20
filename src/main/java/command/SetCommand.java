package command;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class SetCommand implements Command {

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() < 3) {
            client.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
        }

        String key = commandParts.get(1);
        String value = commandParts.get(2);

        if (commandParts.size() == 5 && "PX".equalsIgnoreCase(commandParts.get(3))) {
            long timeMillis = Long.parseLong(commandParts.get(4));

            if (timeMillis <= 0) {
                client.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
            }

            store.set(key, value, timeMillis);
            client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
        } else if (commandParts.size() == 3) {
            store.set(key, value);
            client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
        } else {
            client.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
        }
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'SET'");

    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'SET'");

    }

}
