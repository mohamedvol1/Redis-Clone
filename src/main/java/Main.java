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



public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    	//Uncomment this block to pass the first stage
    	ServerSocketChannel serverSocketChannel = null;
    	Selector selector = null;
        HashSet<SocketChannel> clients = new HashSet<>();
		ByteBuffer buffer = ByteBuffer.allocate(256);
        
        int port = 6379;
        try {
        	
	    	serverSocketChannel =  ServerSocketChannel.open();
	    	selector = Selector.open();
	    	serverSocketChannel.configureBlocking(false);
	    	// ensures that we don't run into 'Address already in use' errors
	    	serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
	    	serverSocketChannel.bind(new InetSocketAddress(port));
	    	serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
          
	    	while (true) {
	    		if (selector.select() == 0) {
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
	    						System.out.println("Disconneted: " + clientInfo);
	    						client.close();
	    						clients.remove(client);
	    						continue;
	    					}
	    					
	    					buffer.flip();
	    					String message = new String(buffer.array(), buffer.position(), bytesRead);
	    					buffer.clear();
	    					
	    					if ("PING".equalsIgnoreCase(parseRESP2(message))) {
	    						client.write(ByteBuffer.wrap("+PONG\r\n".getBytes()));
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
    	    
    	    if (serverSocketChannel != null) serverSocketChannel.close();
    	    
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
  
  // Parses only array type data (to be modified)
  private static String parseRESP2(String input) {
	    if (input.startsWith("*")) { // Array
	        String[] parts = input.split("\r\n");
	        return parts.length > 2 ? parts[2] : ""; // Extract correct command
	    } else if (input.startsWith("$")) { // Bulk string
	        String[] parts = input.split("\r\n");
	        return parts.length > 1 ? parts[1] : ""; // Extract correct command
	    }
	    return input; // Default fallback
	}
}
