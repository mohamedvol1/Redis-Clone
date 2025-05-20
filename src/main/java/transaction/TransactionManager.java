package transaction;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.ArrayDeque;

public class TransactionManager {
    private static final Map<SocketChannel, Queue<List<String>>> transactionMap = new HashMap<>();

    public static boolean isInTransaction(SocketChannel client) {
        return transactionMap.containsKey(client);
    }

    public static void startTransaction(SocketChannel client) {
        transactionMap.put(client, new ArrayDeque<>());
    }

    public static void queueCommand(SocketChannel client, List<String> commandParts) {
        Queue<List<String>> queue = transactionMap.get(client);
        if (queue != null) {
            queue.offer(commandParts);
        }
    }

    public static Queue<List<String>> removeQueuedCommands(SocketChannel client) {
        return transactionMap.remove(client);
    }

    public static void abortTransaction(SocketChannel client) {
        transactionMap.remove(client);
    }
}

