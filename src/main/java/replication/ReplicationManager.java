package replication;

import config.Config;
import protocol.RESPParser;
import replication.wait.WaitRequest;
import store.DataStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;

public class ReplicationManager {
    private final Config config;
    private final DataStore store;
    private Selector selector;

    // only counts propagated commands on master and replicas
    private long processedCommandOffset = 0;
    private final Map<SocketChannel, Long> replicaAcks = new HashMap<>();

    // not used currently
    // private long lastAckTime = 0;
    // private static final long ACK_INTERVAL_MS = 1000;


    private SocketChannel masterConnection;
    private final Map<SocketChannel, ReplicationState> replicaStates = new HashMap<>();

    private final List<SocketChannel> activeReplicas = new ArrayList<>();

    private final List<WaitRequest> pendingWaits = new ArrayList<>();

    public enum ReplicationState {
        CONNECTING,
        WAIT_PONG,
        WAIT_REPLCONF_PORT_REPLY,
        WAIT_REPLCONF_CAPA_REPLY,
        WAIT_PSYNC,
        REPLICATION_ACTIVE
    }

    private ReplicationState handshakeState;

    public ReplicationManager(Config config, DataStore store) {
        this.config = config;
        this.store = store;
    }

    public void initializeReplica() throws IOException {
        String replicaof = config.get("replicaof");
        if (replicaof != null) {
            String[] masterInfo = replicaof.split(" ");
            if (masterInfo.length == 2) {
                String host = masterInfo[0];
                int port = Integer.parseInt(masterInfo[1]);
                int replicaPort = Integer.parseInt(config.get("port"));
                connectToMaster(host, port, replicaPort);
            }
        }
    }

    public void connectToMaster(String host, int port, int replicaPort) throws IOException {
        System.out.println("\u001B[33mConnecting to master at " + host + ": " + port + " ...\u001B[0m");

        masterConnection = SocketChannel.open();
        masterConnection.configureBlocking(false);
        masterConnection.connect(new InetSocketAddress(host, port));

        handshakeState = ReplicationState.CONNECTING;

        masterConnection.register(selector, SelectionKey.OP_CONNECT);
    }

    public void initiateMasterReplication(SelectionKey key) throws IOException {
        if (handshakeState == ReplicationState.CONNECTING) {
            if (masterConnection.finishConnect()) { // make sure we are connected to the master
                // TODO: this code to be moved to PingCommand class and the same other commnads
                String pingCommand = "*1\r\n$4\r\nPING\r\n"; // notice this is RESP array -> different from client to master
                masterConnection.write(ByteBuffer.wrap(pingCommand.getBytes()));

                handshakeState = ReplicationState.WAIT_PONG;
                key.interestOps(SelectionKey.OP_READ);
            } else {
                System.out.println("\u001B[31mFailed to connect to master\u001B[0m");
                key.cancel();
                masterConnection.close();
            }
        }
    }

    public void handleMasterResponse(SocketChannel sc, String response) throws IOException {
        switch (handshakeState) {
            case WAIT_PONG:
                if (response.startsWith("+PONG")) {
                    // TODO: to be refactored
                    handshakeState = ReplicationState.WAIT_REPLCONF_PORT_REPLY;
                    String replConfCommand = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + config.get("port").length() + "\r\n" + config.get("port") + "\r\n";
                    sc.write(ByteBuffer.wrap(replConfCommand.getBytes()));
                } else {
                    System.out.println("\u001B[31mReceived invalid PONG from master\u001B[0m");
                }
                break;

            case WAIT_REPLCONF_PORT_REPLY:
                if (response.startsWith("+OK")) {
                    handshakeState = ReplicationState.WAIT_REPLCONF_CAPA_REPLY;
                    String replConfCapaCommand = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                    sc.write(ByteBuffer.wrap(replConfCapaCommand.getBytes()));
                } else {
                    System.out.println("\u001B[31mReceived invalid OK from master\u001B[0m");
                }
                break;

            case WAIT_REPLCONF_CAPA_REPLY:
                if (response.startsWith("+OK")) {
                    handshakeState = ReplicationState.WAIT_PSYNC;
                    String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                    sc.write(ByteBuffer.wrap(psyncCommand.getBytes()));
                } else {
                    System.out.println("\u001B[31mReceived invalid OK from master\u001B[0m");
                }
                break;

            case WAIT_PSYNC:
                if (response.startsWith("+FULLRESYNC")) {
                    handshakeState = ReplicationState.REPLICATION_ACTIVE;
                    // there probably should be some logic to parse the RDB file sent by master
                    // sometimes master sent GETACK request with final step of handshake (not only after activation)
                    if (response.contains("REPLCONF") && response.contains("GETACK")) {
                        String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + String.valueOf(processedCommandOffset).length() + "\r\n" + processedCommandOffset + "\r\n";
                        sc.write(ByteBuffer.wrap(ackResponse.getBytes()));
                        processedCommandOffset += calculateCommandLength("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"); // we can just do +37 since this value wont changes but only for testing purposes
                    }
                } else {
                    System.out.println("\u001B[31mUnexpected PSYNC response: " + response + "\u001B[0m");
                }
                break;

            case REPLICATION_ACTIVE:
                // parse master commands and process them
                // TODO: to be refactored
                try {
                    // Check if the response contains RDB data followed by commands
                    if (response.contains("REDIS0011")) {
                        // Extract and process any commands that come after the RDB data
                        int asteriskPos = response.indexOf("*", response.indexOf("REDIS0011"));

                        if (asteriskPos != -1) {
                            response = response.substring(asteriskPos);
                        }
                    }
                    List<List<String>> commands = RESPParser.processBufferData(response);

                    for (List<String> command : commands) {
                        String commandName = command.get(0);

                        if ("REPLCONF".equalsIgnoreCase(commandName) && command.size() > 1 && "GETACK".equalsIgnoreCase(command.get(1))) {
                            String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + String.valueOf(processedCommandOffset).length() + "\r\n" + processedCommandOffset + "\r\n";
                            sc.write(ByteBuffer.wrap(ackResponse.getBytes()));
                            processedCommandOffset += calculateCommandLength("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                            continue;
                        }

                        if ("PING".equalsIgnoreCase(commandName)) {
                            processedCommandOffset += calculateCommandLength("*1\r\n$4\r\nPING\r\n");
                            continue;
                        }

                        // process the rest commands (only set commands for now)
                        String key = command.get(1);
                        String value = command.get(2);

                        if (command.size() == 5 && "PX".equalsIgnoreCase(command.get(3))) {
                            long timeMillis = Long.parseLong(command.get(4));

                            if (timeMillis <= 0) {
                                // to modified to be more descriptive (for replica)
                                sc.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
                            }

                            store.set(key, value, timeMillis);
                            processedCommandOffset += calculateCommandLength(command);
                        } else if (command.size() == 3) {
                            store.set(key, value);
                            processedCommandOffset += calculateCommandLength(command);
                        } else {
                            // to modified to be more descriptive (for replica)
                            sc.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
                        }
                    }
                } catch (Exception e) {
                    System.out.println("\u001B[31mError executing master command: " + e.getMessage() + "\u001B[0m");
                }
                break;
        }

        return;
    }


    public void propagateCommand(List<String> commandParts) throws IOException {
        // count propagated commands by master server
        processedCommandOffset += calculateCommandLength(commandParts);

        if (activeReplicas.isEmpty()) {
            return;
        }



        // Format command in RESP protocol
        StringBuilder cmd = new StringBuilder();
        cmd.append("*").append(commandParts.size()).append("\r\n");
        for (String part : commandParts) {
            cmd.append("$").append(part.length()).append("\r\n").append(part).append("\r\n");
        }

        ByteBuffer respBuffer = ByteBuffer.wrap(cmd.toString().getBytes());

        // Send to all active replicas
        for (SocketChannel replica : activeReplicas) {
            if (respBuffer.position() > 0) {
                respBuffer.rewind();
            }
            replica.write(respBuffer);
        }
    }

    public void setSelector(Selector selector) {
        this.selector = selector;
    }

    public Selector getSelector() {
        return this.selector;
    }

    public SocketChannel getMasterConnection() {
        return this.masterConnection;
    }

    public void addReplica(SocketChannel replica) {
        replicaStates.put(replica, ReplicationState.WAIT_PONG);
    }

    public void updateReplicaState(SocketChannel replica, ReplicationState state) {
        replicaStates.put(replica, state);
        if (state == ReplicationState.REPLICATION_ACTIVE) {
            activeReplicas.add(replica);
        }
    }

    private int calculateCommandLength(List<String> command) {
        int length = 0;

        // Calculate header length: *<num>\r\n
        length += 1 + String.valueOf(command.size()).length() + 2;

        // Calculate length for each argument
        for (String arg : command) {
            // $<length>\r\n
            length += 1 + String.valueOf(arg.length()).length() + 2;
            // argument + \r\n
            length += arg.length() + 2;
        }

        return length;
    }

    private int calculateCommandLength(String rawCommand) {
        return rawCommand.getBytes().length;
    }


    public void removeReplica(SocketChannel replica) {
        replicaStates.remove(replica);
        activeReplicas.remove(replica);
    }

    public int getActiveReplicasCount() {
        return activeReplicas.size();
    }

//    public void sendPeriodicAckToMaster() throws IOException {
//        System.out.println("\u001B[35m>>>>>>>>>>>>>sendPeriodicAckToMaster:");
//        // Only do this if a replica and connected to a master
//        if (masterConnection != null && handshakeState == ReplicationState.REPLICATION_ACTIVE) {
//            long now = System.currentTimeMillis();
//            if (now - lastAckTime >= ACK_INTERVAL_MS) {
//                String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" +
//                        String.valueOf(processedCommandOffset).length() +
//                        "\r\n" + processedCommandOffset + "\r\n";
//                masterConnection.write(ByteBuffer.wrap(ackResponse.getBytes()));
//                lastAckTime = now;
//                System.out.println("\u001B[35mSent periodic ACK to master with offset: " +
//                        processedCommandOffset + "\u001B[0m");
//            }
//        }
//    }

    public long getCurrentCommandOffset() {
        return processedCommandOffset;
    }

    public void handleReplicaAck(SocketChannel replica, long ackOffset) {
        replicaAcks.put(replica, ackOffset);
    }

    public int countAcksForCommand(long commandOffset) {
        int count = 0;
        for (Long ackOffset : replicaAcks.values()) {
            if (ackOffset >= commandOffset) {
                count++;
            }
        }
        return count;
    }

    public void registerWait(SocketChannel client, int numReplicas, int timeout) {
        System.out.println("\033[35m>>>>>> registerWait: " + numReplicas + " - " + timeout + "\033[0m");
        long currentOffset = getCurrentCommandOffset();
        System.out.println("\033[35m>>>>>> current offset (server): " + currentOffset + "\033[0m");
        long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;
        pendingWaits.add(new WaitRequest(client, numReplicas, currentOffset, deadline));
    }

    public void processPendingWaits() {
        System.out.println("\033[35m>>>>>> processPendingWaits: " + pendingWaits + "\033[0m");
        if (pendingWaits.isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();
        Iterator<WaitRequest> itr = pendingWaits.iterator();

        while (itr.hasNext()) {
            WaitRequest wait = itr.next();
            int ackedReplicas = countAcksForCommand(processedCommandOffset);
            SocketChannel sc = wait.getClient();

            // Check if we have enough acks or if we have timed out
            if (wait.isExpired(now) || (ackedReplicas >= wait.getRequiredReplicas())) {
                try {
                    // Send the response with the number of acked replicas
                    int numReplies = Math.min(ackedReplicas, wait.getRequiredReplicas());
                    String response = ":" + numReplies + "\r\n";
                    wait.getClient().write(ByteBuffer.wrap(response.getBytes()));
                    itr.remove();
                } catch (IOException e) {
                    System.out.println("Error responding to WAIT command: " + e.getMessage());
                    itr.remove();
                }
            }
        }
    }

    public boolean hasWaitRequest(SocketChannel client) {
        for (WaitRequest wait : pendingWaits) {
            if (wait.getClient().equals(client)) {
                return true;
            }
        }
        return false;
    }

    public void sendGetackToActiveSlaves() throws IOException {
        String getackCommand = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        ByteBuffer getackBuffer = ByteBuffer.wrap(getackCommand.getBytes());

        for (SocketChannel replica : activeReplicas) {
            if (replica.isOpen()) { // Check if replica is still connected
                if (getackBuffer.position() > 0) {
                    getackBuffer.rewind(); // Reset buffer position
                }
                replica.write(getackBuffer);
            }
        }
    }



}