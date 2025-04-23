package command;

import java.util.HashMap;
import java.util.Map;

import config.Config;

public class CommandRegistry {
	private final Map<String, Command> commands = new HashMap<>();

	public CommandRegistry() {
		registerCommand("PING", new PingCommand());
		registerCommand("SET", new SetCommand());
		registerCommand("GET", new GetCommand());
		registerCommand("ECHO", new EchoCommand());
	}

	public CommandRegistry(Config config) {
		this();
		registerCommand("CONFIG", new ConfigCommand(config));
		registerCommand("KEYS", new KeysCommand());
		registerCommand("INFO", new InfoCommand(config));
		registerCommand("REPLCONF", new ReplconfCommand());
		registerCommand("PSYNC", new PsyncCommand());
	}

	private void registerCommand(String name, Command command) {
		commands.put(name, command);
	}

    public Command getCommand(String name) {
        return commands.get(name.toUpperCase());
    }

}
