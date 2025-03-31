import java.io.IOException;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.List;

import protocol.RESPParser;
import store.DataStore;

public class Main {
	private static final int CLEANUP_INTERVAL_MS = 100;
	private static final int SAMPLE_SIZE = 20;
	private static final int EXPIRY_THRESHOLD = 25;

	public static void main(String[] args) {
		// You can use print statements as follows for debugging, they'll be visible
		// when running tests.
		System.out.println("Logs from your program will appear here!");

		// Uncomment this block to pass the first stage
		ServerSocketChannel serverSocketChannel = null;
		Selector selector = null;
		HashSet<SocketChannel> clients = new HashSet<>();
		ByteBuffer buffer = ByteBuffer.allocate(256);
		DataStore store = new DataStore();
		long lastCleanupTime = System.currentTimeMillis();

		int port = 6379;
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
								if ("PING".equalsIgnoreCase(command) && commandParts.size() == 1) {
									client.write(ByteBuffer.wrap("+PONG\r\n".getBytes()));
								} else if ("ECHO".equalsIgnoreCase(command) && commandParts.size() == 2) {
									String argument = commandParts.get(1);
									String response = "$" + argument.length() + "\r\n" + argument + "\r\n";
									client.write(ByteBuffer.wrap(response.getBytes()));
								} else if ("SET".equalsIgnoreCase(command) && commandParts.size() == 3) {
									String k = commandParts.get(1);
									String v = commandParts.get(2);
									store.set(k, v);
									client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
								} else if ("SET".equalsIgnoreCase(command) && "PX".equalsIgnoreCase(commandParts.get(3))
										&& commandParts.size() == 5) {
									String k = commandParts.get(1);
									String v = commandParts.get(2);
									String expTime = commandParts.get(4);

									if (k == null || v == null || expTime == null) {
										client.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
									}

									long timeMillis = Long.parseLong(expTime);

									if (timeMillis <= 0) {
										client.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
									} else {
										store.set(k, v, timeMillis);
										client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
									}

								} else if ("GET".equalsIgnoreCase(command) && commandParts.size() == 2) {
									String k = commandParts.get(1);
									String v = (String) store.get(k);
									if (v != null) {
										String response = "$" + v.length() + "\r\n" + v + "\r\n";
										client.write(ByteBuffer.wrap(response.getBytes()));
									} else {
										client.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
									}
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
}
