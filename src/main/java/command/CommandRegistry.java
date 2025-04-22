package command;

import java.util.HashMap;
import java.util.Map;

public class CommandRegistry {
	private final Map<String, Command> commands = new HashMap<>();
	
	public CommandRegistry() {
		registerCommand("PING", new PingCommand());
		registerCommand("SET", new SetCommand());
		registerCommand("GET", new GetCommand());
		registerCommand("ECHO", new EchoCommand());
	}
	
	private void registerCommand(String name, Command command) {
		commands.put(name, command);
	}
	
    public Command getCommand(String name) {
        return commands.get(name.toUpperCase());
    }

}
