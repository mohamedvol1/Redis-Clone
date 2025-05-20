package command.transactions;

import command.Command;
import store.DataStore;
import transaction.TransactionManager;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class MultiCommand implements Command {

    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        if (commandParts.size() != 1) {
            throw new Exception("ERR wrong number of arguments for 'MULTI' command");
        }

        if (TransactionManager.isInTransaction(client)) {
            throw new Exception("ERR already in multi mode");
        }

        TransactionManager.startTransaction(client);

        client.write(ByteBuffer.wrap("+OK\r\n".getBytes()));
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'MULTI'");
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong number of arguments for 'MULTI'");
    }
}
