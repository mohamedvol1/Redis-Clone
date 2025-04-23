package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import config.Config;
import store.DataStore;

public class InfoCommand implements Command {
    private static final String MASTER_REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final int MASTER_REPLICATION_OFFSET = 0;
    
    private final Config config;
    
    public InfoCommand(Config config) {
        this.config = config;
    }
    
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        execute(client, commandParts);
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        try {
            if (commandParts.size() != 2) {
                throw new Exception("ERR wrong number of arguments for 'INFO' command");
            }

            if ("replication".equalsIgnoreCase(commandParts.get(1))) {
                String role = config.get("replicaof") != null ? "slave" : "master";  
                StringBuilder response = new StringBuilder();
                response.append("role:").append(role).append("\r\n");
                
                if ("master".equals(role)) {
                    response.append("master_replid:").append(MASTER_REPLICATION_ID).append("\r\n");
                    response.append("master_repl_offset:").append(MASTER_REPLICATION_OFFSET);
                }
                
                String bulkString = "$" + response.length() + "\r\n" + response.toString() + "\r\n";
                client.write(ByteBuffer.wrap(bulkString.getBytes()));
            } else {
                throw new Exception("ERR unsupported INFO section");
            }
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing INFO: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'INFO' command");
    }
}