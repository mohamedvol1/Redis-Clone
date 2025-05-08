// this class is not in use now but it will be helpful in future

package replication.wait;

import java.nio.channels.SocketChannel;

public class WaitRequest {
    private final SocketChannel client;
    private final int requiredReplicas;
    private final long commandOffset;
    private final long deadline;
    private boolean completed = false;

    public WaitRequest(SocketChannel client, int requiredReplicas, long commandOffset, long timeoutMillis) {
        this.client = client;
        this.requiredReplicas = requiredReplicas;
        this.commandOffset = commandOffset;
        this.deadline = timeoutMillis;
    }

    public boolean isExpired(long currentTime) {
        return currentTime >= deadline;
    }

    public SocketChannel getClient() {
        return client;
    }

    public int getRequiredReplicas() {
        return requiredReplicas;
    }

    public long getCommandOffset() {
        return commandOffset;
    }

    public long getDeadline() {
        return deadline;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WaitRequest that = (WaitRequest) o;

        if (requiredReplicas != that.requiredReplicas) return false;
        if (commandOffset != that.commandOffset) return false;
        return client.equals(that.client);
    }

    @Override
    public int hashCode() {
        int result = client.hashCode();
        result = 31 * result + requiredReplicas;
        result = 31 * result + (int) (commandOffset ^ (commandOffset >>> 32));
        return result;
    }
}
