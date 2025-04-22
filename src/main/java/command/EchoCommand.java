package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public class EchoCommand implements Command {
	@Override
	public void execute(SocketChannel client, List<String> commandParts) throws Exception {
		try {
			String argument = commandParts.get(1);
			String response = "$" + argument.length() + "\r\n" + argument + "\r\n";
			client.write(ByteBuffer.wrap(response.getBytes()));
		} catch (IOException e) {
			throw new Exception("ERR, somthing went wrong while processing ECHO: " + e.getMessage());
		}
	}

	@Override
	public void execute(SocketChannel client) throws Exception {
		throw new Exception("ERR wrong number of arguments for 'ECHO'");
	}

	@Override
	public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
		throw new Exception("ERR wrong number of arguments for 'ECHO'");
	}
}
