package server;

import command.CommandRegistry;
import command.Command;
import config.Config;
import protocol.RESPParser;
import rdb.RDBFileParser;
import replication.ReplicationManager;
import store.DataStore;
import store.Entry;
import streams.manager.StreamManager;
import transaction.TransactionManager;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.nio.ByteBuffer;
import java.net.Socket;

public class RedisServer {
    private final int port;
    private final Config config;
    private final DataStore store;
    private final CommandRegistry commandRegistry;
    private final ReplicationManager replicationManager;
    private final StreamManager streamManager;

    // TODO: this needs to be refactored
    public static Map<String, List<String>> cmdContext = Map.of(
            "minimalCtx", Arrays.asList("PING"),
            "partialCtx", Arrays.asList("CONFIG", "INFO", "REPLCONF", "PSYNC", "ECHO", "WAIT"),
            "fullCtx", Arrays.asList("SET", "GET", "KEYS", "TYPE", "XADD", "XRANGE", "XREAD", "INCR", "MULTI", "EXEC")
    );


    private static final int CLEANUP_INTERVAL_MS = 1000; // 1 second
    private static final int SAMPLE_SIZE = 20;
    private static final int EXPIRY_THRESHOLD = 25;

    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    private final Set<SocketChannel> clients = new HashSet<>();

    public RedisServer(Config config, DataStore store) {
        this.config = config;
        this.port = Integer.parseInt(config.get("port"));
        this.store = store;
        this.replicationManager = new ReplicationManager(config, store);
        this.streamManager = new StreamManager();
        this.commandRegistry = new CommandRegistry(config, replicationManager, streamManager);
    }

    public void initialize() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        selector = Selector.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.replicationManager.setSelector(selector);

        // Load RDB file if it exists (new change)
        String dir = config.get("dir");
        String dbfilename = config.get("dbfilename");
        String rdbFilePath = dir + "/" + dbfilename;
        File rdbFile = new File(rdbFilePath);
        if (rdbFile.exists()) {
            try {
                Map<String, Entry> data = RDBFileParser.parseFile(rdbFilePath);
                store.load(data);
            } catch (IOException e) {
                System.out.println("Error loading RDB file: " + e.getMessage());
            }
        } else {
            System.out.println("RDB file not found, starting with an empty database.");
        }

        // Initialize replication context if needed
        if (config.get("replicaof") != null) {
            this.replicationManager.initializeReplica();
        }

        System.out.println("\u001B[32mStarting Redis server on port " + port + "\u001B[0m");
    }

    public void start() throws IOException {
        initialize();

        ByteBuffer buffer = ByteBuffer.allocate(4096); // Increased buffer size from original 256
        long lastCleanupTime = System.currentTimeMillis();

        try {
            while (true) {
                // 1. Check for expired keys (periodic cleanup)
                long currentTimeInMillis = System.currentTimeMillis();
                if (currentTimeInMillis > lastCleanupTime + CLEANUP_INTERVAL_MS) {
                    store.activeExpiryCycle(SAMPLE_SIZE, EXPIRY_THRESHOLD);
                    lastCleanupTime = currentTimeInMillis;
                }

                streamManager.processTimedOutRequests();

                // Non-blocking select with timeout
                if (selector.select(100) == 0) {
                    continue;
                }

                // Accept new connections and handle client requests
                for (SelectionKey key : selector.selectedKeys()) {
                    if (key.isAcceptable()) {
                        // Accept new connection
                        if (key.channel() instanceof ServerSocketChannel serverSocket) {
                            SocketChannel client = serverSocket.accept();
                            Socket socket = client.socket();
                            String clientInfo = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                            System.out.println("\u001B[32mCONNECTED: " + clientInfo + "\u001B[0m");
                            client.configureBlocking(false);
                            client.register(selector, SelectionKey.OP_READ);
                            clients.add(client);
                        }
                    } else if (key.isConnectable()) { // is master ready in master-replica connection
                        replicationManager.initiateMasterReplication(key);
                    } else if (key.isReadable()) {
                        // Handle client data
                        if (key.channel() instanceof SocketChannel sc) {
                            int bytesRead = sc.read(buffer);

                            if (bytesRead == -1) {
                                // Client disconnected
                                Socket socket = sc.socket();
                                String clientInfo = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                                System.out.println("Disconnected: " + clientInfo);

                                // Clean up client resources
                                replicationManager.removeReplica(sc);
                                clients.remove(sc);
                                sc.close();
                                key.cancel();
                                continue;
                            }

                            // Process the received data
                            buffer.flip();
                            String message = new String(buffer.array(), buffer.position(), bytesRead);
                            buffer.clear();

                            // replica context (process commands coming from master)
                            if (sc.equals(replicationManager.getMasterConnection())) {
                                replicationManager.handleMasterResponse(sc, message);
                                continue;
                            }


                            // Parse and execute command
                            List<String> commandParts = RESPParser.process(message);
                            if (!commandParts.isEmpty()) {
                                String commandName = commandParts.get(0);
                                Command cmd = commandRegistry.getCommand(commandName);

                                if (cmd != null) {
                                    try {
                                        // check if client is in multi mode (command wont be executed just queued in connection transaction)
                                        if (TransactionManager.isInTransaction(sc) && !"MULTI".equalsIgnoreCase(commandName) && !"EXEC".equalsIgnoreCase(commandName)) {
                                            TransactionManager.queueCommand(sc, commandParts);
                                            sc.write(ByteBuffer.wrap("+QUEUED\r\n".getBytes()));
                                            continue;
                                        }

                                        // Execute command based on its context requirements
                                        if (cmdContext.get("minimalCtx").contains(commandName.toUpperCase())) {
                                            cmd.execute(sc);
                                        } else if (cmdContext.get("partialCtx").contains(commandName.toUpperCase())) {
                                            cmd.execute(sc, commandParts);

                                            // Handle replication state transitions (replconf listening-port, capa, psync)
                                            if ("REPLCONF".equalsIgnoreCase(commandName) && !"ACK".equalsIgnoreCase(commandParts.get(1))) { // this condition needs to be refactored
                                                replicationManager.addReplica(sc);
                                                replicationManager.updateReplicaState(sc, ReplicationManager.ReplicationState.WAIT_PSYNC);
                                            } else if ("PSYNC".equalsIgnoreCase(commandName)) {
                                                replicationManager.updateReplicaState(sc, ReplicationManager.ReplicationState.REPLICATION_ACTIVE);
                                            }

                                        } else if (cmdContext.get("fullCtx").contains(commandName.toUpperCase())) {
                                            cmd.execute(sc, commandParts, store);

                                            // 4. Propagate changes to replicas
                                            if ("SET".equalsIgnoreCase(commandName) || "DEL".equalsIgnoreCase(commandName)) {
                                                // Propagate write commands to replicas
                                                replicationManager.propagateCommand(commandParts);
                                            }
                                        } else {
                                            // Unknown command context
                                            sendErrorResponse(sc, "unknown command '" + commandName + "'");
                                        }
                                    } catch (Exception e) {
                                        sendErrorResponse(sc, e.getMessage());
                                    }
                                } else {
                                    // Command not found in registry
                                    sendErrorResponse(sc, "unknown command '" + commandName + "'");
                                }
                            }
                        }
                    }
                }

//                System.out.println("\033[35m>>>>>> while loop: \033[0m");
//
//                try {
//                    if (config.get("replicaof") != null) { // replica server
//                        replicationManager.sendPeriodicAckToMaster();
//                    } else {
//                        replicationManager.processPendingWaits(); // master server
//                    }
//                } catch (IOException e) {
//                    System.out.println("Error in replication tasks: " + e.getMessage());
//                }


                // Clear processed keys
                selector.selectedKeys().clear();
            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
            throw e;
        } finally {
            try {
                // clean up
                for (SocketChannel client : clients) {
                    client.close();
                }

                if (serverSocketChannel != null)
                    serverSocketChannel.close();

            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    private void sendErrorResponse(SocketChannel client, String errorMessage) throws IOException {
        String response = "-ERR " + errorMessage + "\r\n";
        client.write(ByteBuffer.wrap(response.getBytes()));
    }
}