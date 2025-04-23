package command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import config.Config;
import store.DataStore;

public class ConfigCommand implements Command {
    private final Config config;

    public ConfigCommand(Config config) {
        this.config = config;
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        execute(client, commandParts);
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        try {
            if (commandParts.size() < 3) {
                throw new Exception("ERR wrong number of arguments for 'CONFIG' command");
            }

            if ("GET".equalsIgnoreCase(commandParts.get(1))) {
                List<String> params = commandParts.subList(2, commandParts.size());
                StringBuilder response = new StringBuilder();
                response.append("*").append(2 * params.size()).append("\r\n");

                for (String param : params) {
                    String value = config.get(param);
                    if (value != null) {
                        response.append("$").append(param.length()).append("\r\n").append(param)
                                .append("\r\n");
                        response.append("$").append(value.length()).append("\r\n").append(value)
                                .append("\r\n");
                    } else {
                        // Error response
                        response.append("$-1\r\np");
                    }
                }

                client.write(ByteBuffer.wrap(response.toString().getBytes()));
            } else {
                throw new Exception("ERR unsupported CONFIG command");
            }
        } catch (IOException e) {
            throw new Exception("ERR something went wrong while processing CONFIG: " + e.getMessage());
        }
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'CONFIG' command");
    }
}