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
	private static final String MASTER_REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	private static final int MASTER_REPLICATION_OFFSET = 0;

	public static void main(String[] args) throws Exception {
		// You can use print statements as follows for debugging, they'll be visible
		// when running tests.
		System.out.println("Logs from your program will appear here!");

		Config config = new Config(args);
		DataStore store = new DataStore();
		CommandRegistry cmdRegistry = new CommandRegistry();

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
							clients.add(client);
						}
					} else if (key.isReadable()) {
						if (key.channel() instanceof SocketChannel client) {
							int bytesRead = client.read(buffer);

							if (bytesRead == -1) {
								// Server disconnected
								Socket socket = client.socket();
								String clientInfo = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
								System.out.println("Disconnected: " + clientInfo);
								client.close();
								clients.remove(client);
								continue;
							}

							buffer.flip();
							String message = new String(buffer.array(), buffer.position(), bytesRead);
							buffer.clear();

							List<String> commandParts = RESPParser.process(message);
							if (!commandParts.isEmpty()) {
								String command = commandParts.get(0);
								Command cmd = cmdRegistry.getCommand(command);
								if ("PING".equalsIgnoreCase(command) && commandParts.size() == 1) {
									cmd.execute(client);
								} else if ("ECHO".equalsIgnoreCase(command) && commandParts.size() == 2) {
									System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + command);
									cmd.execute(client, commandParts);
								}	
								else if (Arrays.asList("SET", "GET").contains(command)) {			
									cmd.execute(client, commandParts, store);
								} else if ("CONFIG".equalsIgnoreCase(command) && commandParts.size() >= 3) {
									if ("GET".equalsIgnoreCase(commandParts.get(1))) {
										List<String> params = commandParts.subList(2, commandParts.size());
										StringBuilder response = new StringBuilder();
										response.append("*").append(2 * params.size()).append("\r\n");

										for (String param : params) {
											String value = config.get(param);
											if (value != null) {
												response.append("$").append(param.length()).append("\r\n").append(param)
														.append("\r\n");
												response.append("$").append(value.length()).append("\r\n").append(value)
														.append("\r\n");
											} else {
												// Error response
												response.append("$").append(param.length()).append("\r\n").append(param)
														.append("\r\n");
												response.append("$-1\r\n");
											}
										}

										client.write(ByteBuffer.wrap(response.toString().getBytes()));
									}
								} else if ("KEYS".equalsIgnoreCase(command) && commandParts.size() == 2
										&& "*".equals(commandParts.get(1))) {
									List<String> keys = store.getAllKeys();
									StringBuilder response = new StringBuilder();
									response.append("*").append(keys.size()).append("\r\n");
									for (String k : keys) {
										response.append("$").append(k.length()).append("\r\n").append(k).append("\r\n");
									}
									client.write(ByteBuffer.wrap(response.toString().getBytes()));
								} else if ("INFO".equalsIgnoreCase(command) && commandParts.size() == 2 && "replication".equalsIgnoreCase(commandParts.get(1))) {
									String role = config.get("replicaof") != null ? "slave" : "master";  
									StringBuilder response = new StringBuilder();
									response.append("role:").append(role).append("\r\n");
									
									if ("master".equals(role)) {
										response.append("master_replid:").append(MASTER_REPLICATION_ID);
										response.append("master_repl_offset:").append(MASTER_REPLICATION_OFFSET);
									}
									
									String bulkString = "$" + response.length() + "\r\n" + response.toString() + "\r\n";
									client.write(ByteBuffer.wrap(bulkString.toString().getBytes()));
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
