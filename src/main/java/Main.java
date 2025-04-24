import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import command.Command;
import command.CommandRegistry;
import config.Config;
import protocol.RESPParser;
import rdb.RDBFileParser;
import store.DataStore;
import store.Entry;

public class Main {
    private static final int CLEANUP_INTERVAL_MS = 100;
    private static final int SAMPLE_SIZE = 20;
    private static final int EXPIRY_THRESHOLD = 25;

    private static Map<SocketChannel, ReplicationState> replicaStates = new HashMap<>();
    private static List<SocketChannel> activeReplicas = new ArrayList<>();

    private static Map<String, List<String>> cmdContext = new HashMap<>();

    private static enum ReplicationState {
        WAIT_PONG,
        WAIT_REPLCONF,
        WAIT_PSYNC,
        REPLICATION_ACTIVE
    }

    public static void main(String[] args) throws Exception {
        // You can use print statements as follows for debugging, they'll be visible
        // when running tests.
        System.out.println("Logs from your program will appear here!");

        Config config = new Config(args);
        DataStore store = new DataStore();
        CommandRegistry cmdRegistry = new CommandRegistry(config);

        // intializing commands with contexts
        cmdContext.put("minimalCtx", Arrays.asList("PING"));
        cmdContext.put("partialCtx", Arrays.asList("CONFIG", "INFO", "REPLCONF", "PSYNC", "ECHO"));
        cmdContext.put("fullCtx", Arrays.asList("SET", "GET", "KEYS"));

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

        String replicaof = config.get("replicaof");

        if (replicaof != null) {
            String[] masterInfo = replicaof.split(" ");

            if (masterInfo.length == 2) {
                String host = masterInfo[0];
                int port = Integer.parseInt(masterInfo[1]);
                int replicaPort = Integer.parseInt(config.get("port"));

                connectToMaster(host, port, replicaPort);
            } else {
                System.out.println("Invalide replica formate. Expected: <host> <port>");
            }
        }

        ServerSocketChannel serverSocketChannel = null;
        Selector selector = null;
        HashSet<SocketChannel> clients = new HashSet<>();
        ByteBuffer buffer = ByteBuffer.allocate(256);
        long lastCleanupTime = System.currentTimeMillis();
        int port = Integer.parseInt(config.get("port"));
        System.out.println("Starting Redis server on port " + port);

        try {

            serverSocketChannel = ServerSocketChannel.open();
            selector = Selector.open();
            serverSocketChannel.configureBlocking(false);
            // ensures that we don't run into 'Address already in use' errors
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverSocketChannel.bind(new InetSocketAddress(port));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                long currentTimeInMillis = System.currentTimeMillis();
                if (currentTimeInMillis > lastCleanupTime + CLEANUP_INTERVAL_MS) {
                    store.activeExpiryCycle(SAMPLE_SIZE, EXPIRY_THRESHOLD);
                    lastCleanupTime = currentTimeInMillis;
                }

                if (selector.select(100) == 0) {
                    continue;
                }

                for (SelectionKey key : selector.selectedKeys()) {
                    if (key.isAcceptable()) {
                        if (key.channel() instanceof ServerSocketChannel serverSocket) {
                            SocketChannel client = serverSocket.accept();
                            Socket socket = client.socket();
                            String clientInfo = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                            System.out.println("CONNECTED: " + clientInfo);
                            client.configureBlocking(false);
                            client.register(selector, SelectionKey.OP_READ);
                            if (config.get("replicaof") != null) {
                                // add client to replicas
                                replicaStates.put(client, ReplicationState.WAIT_PONG);
                            } else {
                                // normal client (user)
                                clients.add(client);
                            }
                        }
                    } else if (key.isReadable()) {
                        if (key.channel() instanceof SocketChannel client) {
                            int bytesRead = client.read(buffer);

                            if (bytesRead == -1) {
                                // Client disconnected
                                Socket socket = client.socket();
                                String clientInfo = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
                                System.out.println("Disconnected: " + clientInfo);
                                // remove replicas if existed
                                replicaStates.remove(client);
                                activeReplicas.remove(client);
                                // remove normal client if existed
                                clients.remove(client);
                                client.close();

                                continue;
                            }

                            buffer.flip();
                            String message = new String(buffer.array(), buffer.position(), bytesRead);
                            buffer.clear();

                            List<String> commandParts = RESPParser.process(message);
                            if (!commandParts.isEmpty()) {
                                String command = commandParts.get(0);
                                Command cmd = cmdRegistry.getCommand(command);

                                if (cmd != null) {
                                    try {
                                        if (cmdContext.get("minimalCtx").contains(command.toUpperCase())) {
                                            cmd.execute(client);
                                        } else if (cmdContext.get("partialCtx").contains(command.toUpperCase())) {
                                            cmd.execute(client, commandParts);

                                            if ("REPLCONF".equalsIgnoreCase(command)) {
                                                replicaStates.put(client, ReplicationState.WAIT_PSYNC);
                                            }

                                            if ("PSYNC".equalsIgnoreCase(command)) {
                                                replicaStates.put(client, ReplicationState.REPLICATION_ACTIVE);
                                                activeReplicas.add(client);
                                            }

                                        } else if (cmdContext.get("fullCtx").contains(command.toUpperCase())) {
                                            cmd.execute(client, commandParts, store);

                                            // TODO: Write command (will be modified to support more commands)
                                            if ("SET".equalsIgnoreCase(command)) {
                                                StringBuilder respCommand = new StringBuilder();
                                                respCommand.append("*").append(commandParts.size()).append("\r\n");
                                                for (String part : commandParts) {
                                                    respCommand.append("$").append(part.length()).append("\r\n").append(part).append("\r\n");
                                                }
                                                ByteBuffer respBuffer = ByteBuffer.wrap(respCommand.toString().getBytes());

                                                // Send the serialized RESP command to all active replicas
                                                for (SocketChannel replica : activeReplicas) {
                                                    replica.write(respBuffer);
                                                    respBuffer.rewind();

                                                }
                                            }

                                        } else {
                                            // Unknown command
                                            String errorResponse = "-ERR unknown command '" + command + "'\r\n";
                                            client.write(ByteBuffer.wrap(errorResponse.getBytes()));
                                        }
                                    } catch (Exception e) {
                                        String errorResponse = "-ERR " + e.getMessage() + "\r\n";
                                        client.write(ByteBuffer.wrap(errorResponse.getBytes()));
                                    }
                                } else {
                                    // Command not found in registry
                                    String errorResponse = "-ERR unknown command '" + command + "'\r\n";
                                    client.write(ByteBuffer.wrap(errorResponse.getBytes()));
                                }
                            }
                        }
                    }

                }

                selector.selectedKeys().clear();

            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
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

    private static void connectToMaster(String host, int port, int replPort) {
        try {
            System.out.println("Connecting to master at " + host + ":" + port + " ...");

            // Send PING to master # 1
            SocketChannel masterChannel = SocketChannel.open();
            masterChannel.connect(new InetSocketAddress(host, port));
            String pingCommand = "*1\r\n$4\r\nPING\r\n";
            ByteBuffer pingBuffer = ByteBuffer.wrap(pingCommand.getBytes());
            masterChannel.write(pingBuffer);
            System.out.println("Sent PING to master");

            // Read PONG response from master
            ByteBuffer responseBuffer = ByteBuffer.allocate(10240);
            int bytesRead = masterChannel.read(responseBuffer);
            responseBuffer.flip();
            String pingResponse = new String(responseBuffer.array(), 0, bytesRead);
            System.out.println("Received reponse from master: " + pingResponse);
            responseBuffer.clear();

            // Send REPLCONF with listening port # 2
            String replPortString = String.valueOf(replPort);
            String replConfCommand = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + replPortString + "\r\n";
            ByteBuffer replConfBuffer = ByteBuffer.wrap(replConfCommand.getBytes());
            masterChannel.write(replConfBuffer);
            System.out.println("Sent REPLCONF with listening port to master");

            // Read REPLCONF response
            bytesRead = masterChannel.read(responseBuffer);
            responseBuffer.flip();
            String replConfResponse = new String(responseBuffer.array(), 0, bytesRead);
            System.out.println("Recieved response from master: " + replConfResponse);
            responseBuffer.clear();

            // Send REPLCONF with capa psync2 arguments # 3
            String replConfCapaCommand = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            ByteBuffer replConfCapaBuffer = ByteBuffer.wrap(replConfCapaCommand.getBytes());
            masterChannel.write(replConfCapaBuffer);
            System.out.println("Sent REPLCONF with capa psync2 arguments to master");

            // Read master response
            bytesRead = masterChannel.read(responseBuffer);
            responseBuffer.flip();
            String replConfCapaResponse = new String(responseBuffer.array(), 0, bytesRead);
            System.out.println("Recieved response from master: " + replConfCapaResponse);

            // Send PSYNC command
            String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
            ByteBuffer psyncBuffer = ByteBuffer.wrap(psyncCommand.getBytes());
            masterChannel.write(psyncBuffer);
            System.out.println("Sent PSYNC command to master");

        } catch (IOException e) {
            System.out.println("Faild to connect to master: " + e.getMessage());
        }

    }
}
