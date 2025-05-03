package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import replication.ReplicationManager;
import store.DataStore;

// TODO: this command is not fully implemented yet, it only returns the number of connected replicas. (not timeout handling)
public class WaitCommand implements Command {
    private final ReplicationManager replicationManager;
    
    public WaitCommand(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        execute(client, commandParts);
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        try {
            if (commandParts.size() != 3) {
                throw new Exception("ERR wrong number of arguments for 'WAIT' command");
            }
            
            int numReplicasRequested;
            try {
                numReplicasRequested = Integer.parseInt(commandParts.get(1));
            } catch (NumberFormatException e) {
                throw new Exception("ERR value is not an integer or out of range");
            }
            
            if (numReplicasRequested < 0) {
                throw new Exception("ERR numreplicas can't be negative");
            }

            int connectedReplicas = replicationManager.getActiveReplicasCount();
            // Return the number of connected replicas any way even if it's more than numReplicasRequested'
            String response = ":" + connectedReplicas + "\r\n";
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