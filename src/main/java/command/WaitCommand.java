package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;

import protocol.RESPParser;
import replication.ReplicationManager;
import store.DataStore;

// TODO: this command is not fully implemented yet, it only returns the number of connected replicas. (not timeout handling)
public class WaitCommand implements Command {
    private final ReplicationManager replicationManager;
    private long lastCapturedcommandOffset;

    public WaitCommand(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
        lastCapturedcommandOffset = replicationManager.getCurrentCommandOffset();

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
            int timeoutMillis;
            try {
                numReplicasRequested = Integer.parseInt(commandParts.get(1));
                timeoutMillis = Integer.parseInt(commandParts.get(2));
            } catch (NumberFormatException e) {
                throw new Exception("ERR value is not an integer or out of range");
            }

            if (numReplicasRequested < 0) {
                throw new Exception("ERR numreplicas can't be negative");
            }

            long currentOffset = replicationManager.getCurrentCommandOffset();
            if (isWaitWithNoCommands(currentOffset)) {
                String response = ":" + replicationManager.getActiveReplicasCount() + "\r\n";
                client.write(ByteBuffer.wrap(response.getBytes()));
                return;
            }

            // according to redis docs this should be infinite loop but for now will treat it as timeout and return reached number of acked replicas
            if (timeoutMillis == 0) {
                int ActivedReplicas = replicationManager.countAcksForCommand(currentOffset);
                String response = ":" + ActivedReplicas + "\r\n";
                client.write(ByteBuffer.wrap(response.getBytes()));
                return;
            }

            long startTime = System.currentTimeMillis();
            long deadline = startTime + timeoutMillis;
            // initialize selector (fetch main lopp selector)
            Selector selector = replicationManager.getSelector();
            ByteBuffer buffer = ByteBuffer.allocate(512);

            replicationManager.sendGetackToActiveSlaves();

            // this loop blocks the main loop (blocks processing other clients). This is How Redis does it.
            while (System.currentTimeMillis() < deadline) {
                // Check how many replicas have acknowledged the command
                int ackedReplicas = replicationManager.countAcksForCommand(currentOffset);

                if (ackedReplicas >= numReplicasRequested) {
                    // Send the response and return immediately if the required acks are met
                    String response = ":" + ackedReplicas + "\r\n";
                    client.write(ByteBuffer.wrap(response.getBytes()));
                    return;
                }

                // Wait for replica events with remaining timeout
                long remainingTime = deadline - System.currentTimeMillis();
                if (remainingTime <= 0) break;

                if (selector.select(remainingTime) > 0) {
                    for (SelectionKey key : selector.selectedKeys()) {
                        if (key.isReadable()) {
                            if (key.channel() instanceof SocketChannel sc) {
                                int bytesRead = sc.read(buffer);
                                if (bytesRead == -1) {
                                    return;
                                }
                                buffer.flip();
                                String message = new String(buffer.array(), buffer.position(), bytesRead);
                                buffer.clear();
                                List<String> ackRespons = RESPParser.process(message);
                                if (!ackRespons.isEmpty()) {
                                    if ("REPLCONF".equalsIgnoreCase(ackRespons.get(0)) && "ACK".equalsIgnoreCase(ackRespons.get(1))) {
                                        long ackOffset = Long.parseLong(ackRespons.get(2));
                                        replicationManager.handleReplicaAck(sc, ackOffset);
                                    }
                                }
                            }
                        }
                    }
                    selector.selectedKeys().clear();
                }
            }

            // If we timed out, calculate the final number of acks and send the response
            int finalAckedReplicas = replicationManager.countAcksForCommand(currentOffset);
            String response = ":" + finalAckedReplicas + "\r\n";
            client.write(ByteBuffer.wrap(response.getBytes()));


            // register a wait request (non blocking wait)
            // this code wont be used for now (it works fine)
            // replicationManager.registerWait(client, numReplicasRequested, timeoutMillis);
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing WAIT: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'WAIT' command");
    }

    // checks if wait command is prefixed by another command or just a single commabd to check active replicas
    private boolean isWaitWithNoCommands(long currentOffset) {
        return currentOffset == lastCapturedcommandOffset;
    }
}