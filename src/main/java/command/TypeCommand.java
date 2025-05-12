package command;

import store.DataStore;
import store.DataType;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class TypeCommand implements Command {
    @Override
    public void execute (SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() != 2) {
            throw new Exception("ERR wrong number of arguments for 'TYPE' command");
        }

        String key = commandParts.get(1);
        String type = DataType.NONE.toString();

        if (store.exists(key)) {
            type = store.getDataType(key).toString();
        }

        String response = "+" + type + "\r\n";
        client.write(ByteBuffer.wrap(response.getBytes()));
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'TYPE' command");
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'TYPE' command");
    }
}
