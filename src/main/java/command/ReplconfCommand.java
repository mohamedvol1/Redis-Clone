package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import replication.ReplicationManager;
import store.DataStore;

public class ReplconfCommand implements Command {
    private final ReplicationManager replicationManager;

    public ReplconfCommand(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        execute(client, commandParts);
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        try {
            // handle it silently
            if ("REPLCONF".equalsIgnoreCase(commandParts.get(0)) && "ACK".equalsIgnoreCase(commandParts.get(1))) {
                long ackOffset = Long.parseLong(commandParts.get(2));
                System.out.println("\u001B[35m>>>>>>>>>Received ACK with offset: " + ackOffset + "\u001B[0m");
                replicationManager.handleReplicaAck(client, ackOffset);
                return;
            } else {
                // REPLCONF command always returns OK (to be changed in the future)
                client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
            }
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing REPLCONF: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        execute(client, null);
    }
}