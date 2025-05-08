package command;

import java.util.HashMap;
import java.util.Map;

import config.Config;
import replication.ReplicationManager;

public class CommandRegistry {
	private final Map<String, Command> commands = new HashMap<>();

	public CommandRegistry() {
		registerCommand("PING", new PingCommand());
		registerCommand("SET", new SetCommand());
		registerCommand("GET", new GetCommand());
		registerCommand("ECHO", new EchoCommand());
		registerCommand("KEYS", new KeysCommand());
		registerCommand("PSYNC", new PsyncCommand());
	}

	public CommandRegistry(Config config, ReplicationManager replicationManager) {
		this();
		registerCommand("CONFIG", new ConfigCommand(config));
		registerCommand("INFO", new InfoCommand(config));
		registerCommand("WAIT", new WaitCommand(replicationManager));
		registerCommand("REPLCONF", new ReplconfCommand(replicationManager));
	}

	private void registerCommand(String name, Command command) {
		commands.put(name, command);
	}

    public Command getCommand(String name) {
        return commands.get(name.toUpperCase());
    }

}
