package streams.manager;

import java.nio.channels.SocketChannel;

public class BlockingRequest {
    private final SocketChannel client;
    private final String streamKey;
    private final String startId;
    private final long timeout;
    private final long creationTime;

    public BlockingRequest(SocketChannel client, String streamKey, String startId, long timeout) {
        this.client = client;
        this.streamKey = streamKey;
        this.startId = startId;
        this.timeout = timeout;
        this.creationTime = System.currentTimeMillis();
    }

    public SocketChannel getClient() {
        return client;
    }

    public String getStreamKey() {
        return streamKey;
    }

    public String getStartId() {
        return startId;
    }

    public long getTimeout() {
        return timeout;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public boolean isTimedOut() {
        if (timeout == 0) {
            return false; // never timeout, keep listening
        }
        return System.currentTimeMillis() > creationTime + timeout;
    }
}
