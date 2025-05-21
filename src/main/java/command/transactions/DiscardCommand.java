package command.transactions;

import command.Command;
import store.DataStore;
import transaction.TransactionManager;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class DiscardCommand implements Command {
    @Override
    public void execute(SocketChannel client) throws Exception {
        if (!TransactionManager.isInTransaction(client)) {
            client.write(ByteBuffer.wrap("-ERR DISCARD without MULTI\r\n".getBytes()));
            return;
        }

        TransactionManager.removeTransaction(client);
        client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) {
        throw new RuntimeException("ERR wrong number of arguments for 'DISCARD' command");
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) {
        throw new RuntimeException("ERR wrong number of arguments for 'DISCARD' command");
    }
}
