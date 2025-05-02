package replication;

import config.Config;
import protocol.RESPParser;
import store.DataStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicationManager {
    private final Config config;
    private final DataStore store;
    private Selector selector;

    private SocketChannel masterConnection;
    private final Map<SocketChannel, ReplicationState> replicaStates = new HashMap<>();


    private final List<SocketChannel> activeReplicas = new ArrayList<>();

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
                System.out.println("\u001B[32mConnected to master\u001B[0m");

                // TODO: this code to be moved to PingCommand class and the same other commnads
                String pingCommand = "*1\r\n$4\r\nPING\r\n"; // notice this is RESP array -> different from client to master
                masterConnection.write(ByteBuffer.wrap(pingCommand.getBytes()));
                System.out.println("\u001B[36mSent PING to master\u001B[0m");

                handshakeState = ReplicationState.WAIT_PONG;
                key.interestOps(SelectionKey.OP_READ);
            } else {
                System.out.println("\u001B[31mFailed to connect to master\u001B[0m");
                key.cancel();
                masterConnection.close();
            }
//        } else if (key.isReadable()) {
//            handleMaterResponse(key);
//        }
        }
    }

    public void handleMasterResponse(SocketChannel sc, String response) throws IOException {

        System.out.println("\u001B[34mReceived response from master: " + handshakeState.toString() + "\u001B[0m");

        switch (handshakeState) {
            case WAIT_PONG:
                if (response.startsWith("+PONG")) {
                    // TODO: to be refactored
                    handshakeState = ReplicationState.WAIT_REPLCONF_PORT_REPLY;
                    String replConfCommand = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + config.get("port").length() + "\r\n" + config.get("port") + "\r\n";
                    sc.write(ByteBuffer.wrap(replConfCommand.getBytes()));
                    System.out.println("\u001B[35mSent REPLCONF with listening port to master\u001B[0m");
                } else {
                    System.out.println("\u001B[31mReceived invalid PONG from master\u001B[0m");
                }
                break;

            case WAIT_REPLCONF_PORT_REPLY:
                if (response.startsWith("+OK")) {
                    handshakeState = ReplicationState.WAIT_REPLCONF_CAPA_REPLY;
                    String replConfCapaCommand = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                    sc.write(ByteBuffer.wrap(replConfCapaCommand.getBytes()));
                    System.out.println("\u001B[35mSent REPLCONF with capa psync2 arguments to master\u001B[0m");
                } else {
                    System.out.println("\u001B[31mReceived invalid OK from master\u001B[0m");
                }
                break;

            case WAIT_REPLCONF_CAPA_REPLY:
                if (response.startsWith("+OK")) {
                    handshakeState = ReplicationState.WAIT_PSYNC;
                    String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                    sc.write(ByteBuffer.wrap(psyncCommand.getBytes()));
                    System.out.println("\u001B[35mSent PSYNC to master\u001B[0m");
                } else {
                    System.out.println("\u001B[31mReceived invalid OK from master\u001B[0m");
                }
                break;

            case WAIT_PSYNC:
                if (response.startsWith("+FULLRESYNC")) {
                    handshakeState = ReplicationState.REPLICATION_ACTIVE;
                    // here probably should be some logic to parse the RDB file sent by master

                    // sometimes master sent GETACK request with final step of handshake (not only after activation)
                    if (response.contains("REPLCONF") && response.contains("GETACK")) {
                        System.out.println("\u001B[32mReceived REPLCONF GETACK command after RDB, responding with ACK\u001B[0m");
                        String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";
                        sc.write(ByteBuffer.wrap(ackResponse.getBytes()));
                    }


                } else {
                    System.out.println("\u001B[31mUnexpected PSYNC response: " + response + "\u001B[0m");
                }
                break;

            case REPLICATION_ACTIVE:
                System.out.println("\u001B[31mmaster ready for propagation\u001B[0m");
                // parse master commands and process them
                // TODO: to be refactored
                try {
                    System.out.println("\u001B[31mprocess master commands" + response + "\u001B[0m");
                    List<List<String>> commands = RESPParser.processBufferData(response);
                    System.out.println("\u001B[33mParsed commands structure:\u001B[0m");
                    for (int i = 0; i < commands.size(); i++) {
                        System.out.println("\u001B[33mCommand " + (i + 1) + ":");
                        List<String> command = commands.get(i);
                        for (int j = 0; j < command.size(); j++) {
                            System.out.println("  Arg " + j + ": " + command.get(j));
                        }
                    }
                    

                    for (List<String> command : commands) {
                        String commandName = command.get(0);
                        System.out.println("\u001B[32mExecuting command: " + commandName + "\u001B[0m");

                        if (command.size() < 3) {
                            sc.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
                        }

                        if ("REPLCONF".equalsIgnoreCase(commandName) && command.size() > 1 && "GETACK".equalsIgnoreCase(command.get(1))) {
                            String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";
                            sc.write(ByteBuffer.wrap(ackResponse.getBytes()));
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
                        } else if (command.size() == 3) {
                            store.set(key, value);
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

    }


    public void propagateCommand(List<String> commandParts) throws IOException {
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

    public void removeReplica(SocketChannel replica) {
        replicaStates.remove(replica);
        activeReplicas.remove(replica);
    }
}