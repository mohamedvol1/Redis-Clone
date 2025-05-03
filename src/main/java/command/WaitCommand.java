package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class WaitCommand implements Command {

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'WAIT' command");
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        try {
            if (commandParts.size() != 3) {
                throw new Exception("ERR wrong number of arguments for 'WAIT' command");
            }

            String response = ":0\r\n";
            client.write(ByteBuffer.wrap(response.getBytes()));
            
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing WAIT: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'WAIT' command");
    }
}