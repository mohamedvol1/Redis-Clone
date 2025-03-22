import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;


public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    	//Uncomment this block to pass the first stage
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        BufferedReader reader = null;
        BufferedWriter writer = null;
        
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          // Wait for connection from client.
          clientSocket = serverSocket.accept();
          
          reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
          writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
          String clientMessage = parseRESP2(reader);
          
          if("PING".equalsIgnoreCase(clientMessage)) {
        	  writer.write("+PONG\r\n");
        	  writer.flush();
          }
          
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
        	// clean up
    	    if (reader != null) reader.close();
    	    if (writer != null) writer.close();
    	    if (clientSocket != null) clientSocket.close();
    	    if (serverSocket != null) serverSocket.close();
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
  
  // Parses only array type data (to be modified)
  private static String parseRESP2(BufferedReader reader) throws IOException {
	    String firstLine = reader.readLine();

	    if (firstLine == null) {
	        return null;
	    }

	    if (firstLine.startsWith("*")) {
	        reader.readLine();
	        return reader.readLine();
	    }

	    return firstLine;
	}
}
