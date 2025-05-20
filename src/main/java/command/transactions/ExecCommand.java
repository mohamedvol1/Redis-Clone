package command.transactions;

import command.Command;
import command.CommandRegistry;
import command.transactions.helper.CapturingSocketChannel;
import store.DataStore;
import transaction.TransactionManager;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static server.RedisServer.cmdContext;

public class ExecCommand implements Command {
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() != 1) {
            throw new Exception("ERR wrong number of arguments for 'EXEC' command");
        }

        if (!TransactionManager.isInTransaction(client)) {
            client.write(ByteBuffer.wrap("-ERR EXEC without MULTI\r\n".getBytes()));
            return;
        }

        Queue<List<String>> transaction = TransactionManager.getTransaction(client);

        if (transaction.isEmpty()) {
            // return empty array and remove completely remove transaction (next call with same conditions will throw simple error above)
            client.write(ByteBuffer.wrap("*0\r\n".getBytes()));
            TransactionManager.removeTransaction(client);
            return;
        }

        // collecting responses for queued commands
        List<String> responses = new ArrayList<>();
        SocketChannel sc = new CapturingSocketChannel(responses); // mock socket channel to capture responses

        for (List<String> command : transaction) {
            String cmdName = command.get(0);
            Command cmd = CommandRegistry.getCommand(cmdName);

            if (cmdContext.get("minimalCtx").contains(cmdName.toUpperCase())) {
                cmd.execute(sc);
            } else if (cmdContext.get("partialCtx").contains(cmdName.toUpperCase())) {
                cmd.execute(sc, command);
            } else if (cmdContext.get("fullCtx").contains(cmdName.toUpperCase())) {
                cmd.execute(sc, command, store);
            }
        }

        StringBuilder arrayResponse = new StringBuilder();
        arrayResponse.append("*").append(responses.size()).append("\r\n");
        for (String response : responses) {
            arrayResponse.append(response);
        }

        TransactionManager.removeTransaction(client);
        client.write(ByteBuffer.wrap(arrayResponse.toString().getBytes()));
    }

    public void execute(SocketChannel client, List<String> commandParts) {
        throw new RuntimeException("ERR wrong number of arguments for 'EXEC' command");
    }

    public void execute(SocketChannel client) {
        throw new RuntimeException("ERR wrong number of arguments for 'EXEC' command");
    }
}
