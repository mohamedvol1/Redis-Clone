package command;

import store.DataStore;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class MultiCommand implements Command {
    private final Queue<List<String>> commandsQueue;

    public MultiCommand() {
        this.commandsQueue = new ArrayDeque<>();
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() != 1) {
            throw new Exception("ERR wrong number of arguments for 'MULTI' command");
        }

        client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'MULTI'");
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'MULTI'");
    }
}
