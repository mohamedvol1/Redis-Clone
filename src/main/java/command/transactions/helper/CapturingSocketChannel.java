package command.transactions.helper;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class CapturingSocketChannel extends SocketChannel {
    private final List<String> capturedResponses;

    public CapturingSocketChannel(List<String> responses) {
        super(null);
        this.capturedResponses = responses;
    }

    @Override
    public int write(ByteBuffer src) {
        byte[] response = new byte[src.remaining()];
        src.get(response);
        String responseStr = new String(response, java.nio.charset.StandardCharsets.UTF_8);
        capturedResponses.add(responseStr);
        return response.length;
    }

    // Required override methods (unused)
    @Override public int read(ByteBuffer dst) { return 0; }
    @Override public long read(ByteBuffer[] dsts, int offset, int length) { return 0; }
    @Override public long write(ByteBuffer[] srcs, int offset, int length) { return 0; }
    @Override public SocketChannel bind(java.net.SocketAddress local) { return null; }
    @Override public java.net.Socket socket() { return null; }
    @Override public boolean isConnected() { return true; }
    @Override public boolean isConnectionPending() { return false; }
    @Override public boolean connect(java.net.SocketAddress remote) { return false; }
    @Override public boolean finishConnect() { return false; }
    @Override public java.net.SocketAddress getRemoteAddress() { return null; }
    @Override public java.net.SocketAddress getLocalAddress() { return null; }
    @Override public <T> SocketChannel setOption(java.net.SocketOption<T> name, T value) { return null; }
    @Override public <T> T getOption(java.net.SocketOption<T> name) { return null; }
    @Override public java.util.Set<java.net.SocketOption<?>> supportedOptions() { return null; }
    @Override protected void implCloseSelectableChannel() {}
    @Override protected void implConfigureBlocking(boolean block) {}

    @Override
    public SocketChannel shutdownInput() {
        return null;
    }

    @Override
    public SocketChannel shutdownOutput() {
        return null;
    }


}
