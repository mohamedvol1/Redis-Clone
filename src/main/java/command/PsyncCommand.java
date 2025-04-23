package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class PsyncCommand implements Command {
    private static final String MASTER_REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final int MASTER_REPLICATION_OFFSET = 0;
    
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        execute(client, commandParts);
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        try {
            if (commandParts.size() != 3) {
                throw new Exception("ERR wrong number of arguments for 'PSYNC' command");
            }
            
            String response = "+FULLRESYNC " + MASTER_REPLICATION_ID + " " + MASTER_REPLICATION_OFFSET + "\r\n";
            client.write(ByteBuffer.wrap(response.getBytes()));

            // Send an empty RDB file
            byte[] emptyRDBFile = {
                (byte) 0x52, (byte) 0x45, (byte) 0x44, (byte) 0x49, (byte) 0x53, (byte) 0x30, (byte) 0x30, (byte) 0x31, (byte) 0x31, // RDB header
                (byte) 0xFF
            };
            String rdbHeader = "$" + emptyRDBFile.length + "\r\n";
            client.write(ByteBuffer.wrap(rdbHeader.getBytes()));
            client.write(ByteBuffer.wrap(emptyRDBFile));
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing PSYNC: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'PSYNC' command");
    }
}