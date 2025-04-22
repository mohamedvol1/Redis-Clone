package command;

import java.nio.channels.SocketChannel;
import java.util.List;

import store.DataStore;

public interface Command {
	void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception;
	void execute(SocketChannel client, List<String> commandParts) throws Exception;
	void execute(SocketChannel client) throws Exception;
}
