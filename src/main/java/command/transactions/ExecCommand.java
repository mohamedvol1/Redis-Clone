package command.transactions;

import command.Command;
import store.DataStore;
import transaction.TransactionManager;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Queue;

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

        // fetch the transaction queue (for the first client who called the command)
        // execute the commands atomically (blocking other clients)

        client.write(ByteBuffer.wrap("-ERR EXEC without MULTI\r\n".getBytes()));
    }

    public void execute(SocketChannel client, List<String> commandParts) {
        throw new RuntimeException("ERR wrong number of arguments for 'EXEC' command");
    }

    public void execute(SocketChannel client) {
        throw new RuntimeException("ERR wrong number of arguments for 'EXEC' command");
    }
}
