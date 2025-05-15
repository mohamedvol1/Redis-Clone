package streams.manager;

import streams.Stream;
import streams.StreamEntry;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;

public class StreamManager {
    private Selector selector;

    // Key: stream key, value: list of blocking requests
    private static Map<String, List<BlockingRequest>> pendingRequests;

    public StreamManager() {
        pendingRequests = new HashMap<>();
    }

    public void addBlockRequest(SocketChannel client, String streamKey, String startId, long timeout) {
        BlockingRequest request = new BlockingRequest(client, streamKey, startId, timeout);
        pendingRequests.computeIfAbsent(streamKey, k -> new ArrayList<>()).add(request);
    }

    public void processTimedOutRequests() {
        long currentTime = System.currentTimeMillis();
        Iterator<Map.Entry<String, List<BlockingRequest>>> Itr = pendingRequests.entrySet().iterator();

        // Check all pending requests
        while (Itr.hasNext()) {

            Map.Entry<String, List<BlockingRequest>> entry = Itr.next();
            String streamKey = entry.getKey();
            List<BlockingRequest> requests = entry.getValue();

            for (Iterator<BlockingRequest> requestIt = requests.iterator(); requestIt.hasNext(); ) {
                BlockingRequest request = requestIt.next();

                // Check if request has timed out
                if (request.isTimedOut()) {
                    try {
                        // Send null response for timeout
                        request.getClient().write(ByteBuffer.wrap("$-1\r\n".getBytes()));
                    } catch (Exception e) {
                        System.out.println("Error sending timeout response: " + e.getMessage());
                    }
                    requestIt.remove();
                    continue;
                }
            }

            // Remove stream entry if no more requests
            if (requests.isEmpty()) {
                Itr.remove();
            }
        }
    }


    // wont be used for now (it works as clean up method)
//    public void processPendingRequests() {
//        Iterator<Map.Entry<String, List<BlockingRequest>>> itr = pendingRequests.entrySet().iterator();
//
//        while (itr.hasNext()) {
//            Map.Entry<String, List<BlockingRequest>> entry = itr.next();
//            String streamKey = entry.getKey();
//            List<BlockingRequest> requests = entry.getValue();
//            Stream stream = (Stream) store.get(streamKey);
//
//            Iterator<BlockingRequest> requestIt = requests.iterator();
//            while (requestIt.hasNext()) {
//                BlockingRequest request = requestIt.next();
//
//                if (request.isTimedOut()) {
//                    try {
//                        request.getClient().write(ByteBuffer.wrap("-1\r\n".getBytes()));
//                    } catch (Exception e) {
//                        System.out.println("Stream Manager Error: sending timeout response: " + e.getMessage());
//                    }
//                    continue;
//                }
//
//                if (stream != null) {
//                    try {
//                        List<StreamEntry> entries = stream.getEntriesGreaterThan(request.getStartId());
//
//                        if (!entries.isEmpty()) {
//                            // TODO: this formating code need to be capsulated because it is used in different places
//                            // Build response with new entries
//                            StringBuilder response = new StringBuilder();
//                            response.append("*1\r\n");  // One stream
//
//                            // Stream name and entries array
//                            response.append("*2\r\n");
//                            response.append("$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n");
//
//                            // Entries array
//                            response.append("*").append(entries.size()).append("\r\n");
//
//                            // For each entry in this stream
//                            for (StreamEntry se : entries) {
//                                // Entry ID and fields
//                                response.append("*2\r\n");
//                                response.append("$").append(se.id().length()).append("\r\n").append(se.id()).append("\r\n");
//
//                                // Fields array
//                                Map<String, String> fields = se.fields();
//                                response.append("*").append(fields.size() * 2).append("\r\n");
//
//                                for (Map.Entry<String, String> field : fields.entrySet()) {
//                                    response.append("$").append(field.getKey().length()).append("\r\n").append(field.getKey()).append("\r\n");
//                                    response.append("$").append(field.getValue().length()).append("\r\n").append(field.getValue()).append("\r\n");
//                                }
//                            }
//
//                            // Send response to client
//                            request.getClient().write(ByteBuffer.wrap(response.toString().getBytes()));
//                            // Remove the request as it's been satisfied
//                            requestIt.remove();
//                        }
//                    } catch (Exception e) {
//                        System.out.println("Error processing stream request: " + e.getMessage());
//                        requestIt.remove();
//                    }
//                }
//            }
//        }
//
//    }

    public static Map<String, List<BlockingRequest>> getPendingRequests() {
        return pendingRequests;
    }

    // notify client about new entries in the stream
    public static void notifyNewEntry(String streamKey, Stream stream) {
        List<BlockingRequest> requests = getPendingRequests().get(streamKey);

        if (requests == null || requests.isEmpty()) {
            return;
        }

        List<BlockingRequest> requestsToRemove = new ArrayList<>();

        for (BlockingRequest request : requests) {
            try {
                List<StreamEntry> entries = stream.getEntriesGreaterThan(request.getStartId());

                if (!entries.isEmpty()) {
                    // Format XREAD response
                    StringBuilder response = new StringBuilder();
                    response.append("*1\r\n");  // One stream

                    // Stream name and entries array
                    response.append("*2\r\n");
                    response.append("$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n");

                    // Entries array
                    response.append("*").append(entries.size()).append("\r\n");

                    // For each entry in this stream
                    for (StreamEntry entry : entries) {
                        // Entry ID and fields
                        response.append("*2\r\n");
                        response.append("$").append(entry.id().length()).append("\r\n").append(entry.id()).append("\r\n");

                        // Fields array
                        Map<String, String> fields = entry.fields();
                        response.append("*").append(fields.size() * 2).append("\r\n");

                        for (Map.Entry<String, String> field : fields.entrySet()) {
                            response.append("$").append(field.getKey().length()).append("\r\n").append(field.getKey()).append("\r\n");
                            response.append("$").append(field.getValue().length()).append("\r\n").append(field.getValue()).append("\r\n");
                        }
                    }

                    request.getClient().write(ByteBuffer.wrap(response.toString().getBytes()));
                    requestsToRemove.add(request);
                }
            } catch (Exception e) {
                System.out.println("Error notifying client: " + e.getMessage());
                requestsToRemove.add(request);
            }
        }

        // Remove processed requests
        requests.removeAll(requestsToRemove);
        if (requests.isEmpty()) {
            pendingRequests.remove(streamKey);
        }
    }
}
