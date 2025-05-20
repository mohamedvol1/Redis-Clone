package command;

import java.util.HashMap;
import java.util.Map;

import command.streams.XaddCommand;
import command.streams.XrangeCommand;
import command.streams.XreadCommand;
import command.transactions.ExecCommand;
import command.transactions.MultiCommand;
import config.Config;
import replication.ReplicationManager;
import streams.manager.StreamManager;

public class CommandRegistry {
	private final Map<String, Command> commands = new HashMap<>();

	public CommandRegistry() {
		registerCommand("PING", new PingCommand());
		registerCommand("SET", new SetCommand());
		registerCommand("GET", new GetCommand());
		registerCommand("ECHO", new EchoCommand());
		registerCommand("KEYS", new KeysCommand());
		registerCommand("PSYNC", new PsyncCommand());
		registerCommand("TYPE", new TypeCommand());
		registerCommand("XADD", new XaddCommand());
		registerCommand("XRANGE", new XrangeCommand());
		registerCommand("INCR", new IncermentCommand());
		registerCommand("MULTI", new MultiCommand());
		registerCommand("EXEC", new ExecCommand());
	}

	public CommandRegistry(Config config, ReplicationManager replicationManager, StreamManager streamManager) {
		this();
		registerCommand("CONFIG", new ConfigCommand(config));
		registerCommand("INFO", new InfoCommand(config));
		registerCommand("WAIT", new WaitCommand(replicationManager));
		registerCommand("REPLCONF", new ReplconfCommand(replicationManager));
		registerCommand("XREAD", new XreadCommand(streamManager));
	}

	private void registerCommand(String name, Command command) {
		commands.put(name, command);
	}

    public Command getCommand(String name) {
        return commands.get(name.toUpperCase());
    }

}
