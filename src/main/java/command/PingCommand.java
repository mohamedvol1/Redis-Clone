package command;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class PingCommand implements Command {
	
	@Override
	public void execute(SocketChannel client) throws Exception {
		client.write(ByteBuffer.wrap("+PONG\r\n".getBytes()));
	}
	
	@Override
	public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
	    execute(client);
	}
	
	@Override
	public void execute(SocketChannel client, List<String> commandParts) throws Exception {
		execute(client);
	}

}
