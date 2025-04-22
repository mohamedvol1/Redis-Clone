package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class GetCommand implements Command{
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
    	try {
    		String k = commandParts.get(1);
    		String v = (String) store.get(k);
    		if (v != null) {
    			String response = "$" + v.length() + "\r\n" + v + "\r\n";
    			client.write(ByteBuffer.wrap(response.getBytes()));
    		} else {
    			client.write(ByteBuffer.wrap("$-1\r\n".getBytes()));
    		}
    	} catch (IOException e) {
    		throw new Exception("ERR, somthing went wrong while processing GET: " + e.getMessage());
    	}
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
    	throw new Exception("ERR wrong number of arguments for 'GET'");
    }
    
    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
    	throw new Exception("ERR wrong number of arguments for 'GET'");
    }

}
