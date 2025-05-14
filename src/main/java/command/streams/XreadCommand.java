package command.streams;

import command.Command;
import store.DataStore;
import store.DataType;
import streams.Stream;
import streams.StreamEntry;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XreadCommand implements Command {
    @Override
    public void execute(SocketChannel client, List<String> commandParts, DataStore store) throws Exception {
        // Check if command has minimum required parts (XREAD STREAMS key1 id1)
        if (commandParts.size() < 4) {
            throw new Exception("ERR wrong number of arguments for 'XREAD' command");
        }

        // Check if the command has the STREAMS keyword
        int streamsKeywordIndex = -1;
        for (int i = 1; i < commandParts.size(); i++) {
            if ("STREAMS".equalsIgnoreCase(commandParts.get(i))) {
                streamsKeywordIndex = i;
                break;
            }
        }

        if (streamsKeywordIndex == -1) {
            throw new Exception("ERR syntax error in XREAD command, STREAMS keyword not found");
        }

        // Calculate how many keys and IDs we have
        int keysCount = (commandParts.size() - streamsKeywordIndex - 1) / 2;
        if (keysCount * 2 != commandParts.size() - streamsKeywordIndex - 1) {
            throw new Exception("ERR syntax error in XREAD command, number of streams and IDs don't match");
        }

        // Extract keys and IDs
        List<String> keys = new ArrayList<>();
        List<String> ids = new ArrayList<>();

        for (int i = streamsKeywordIndex + 1; i < streamsKeywordIndex + 1 + keysCount; i++) {
            keys.add(commandParts.get(i));
        }

        for (int i = streamsKeywordIndex + 1 + keysCount; i < commandParts.size(); i++) {
            ids.add(commandParts.get(i));
        }

        // Map to store results for each stream
        Map<String, List<StreamEntry>> streamResults = new HashMap<>();

        // Process each stream and collect entries
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String id = ids.get(i);

            if (!store.exists(key)) {
                // If key doesn't exist, add empty list for this stream
                streamResults.put(key, new ArrayList<>());
                continue;
            }

            if (store.getDataType(key) != DataType.STREAM) {
                throw new Exception("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            Stream stream = (Stream) store.get(key);

            // XREAD is exclusive - only return entries with IDs greater than the provided ID
            List<StreamEntry> entries = stream.getEntriesGreaterThan(id);
            streamResults.put(key, entries);
        }

        // Build RESP response
        StringBuilder response = new StringBuilder();

        // First array length - number of streams with entries
        long streamsWithEntries = streamResults.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .count();

        response.append("*").append(streamsWithEntries).append("\r\n");

        // For each stream with entries
        for (Map.Entry<String, List<StreamEntry>> streamEntry : streamResults.entrySet()) {
            String streamKey = streamEntry.getKey();
            List<StreamEntry> entries = streamEntry.getValue();

            if (entries.isEmpty()) {
                continue;  // Skip streams with no entries (maybe should be changed to return an empty array without skipping)
            }

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
        }

        // If no stream has entries, return an empty array
        if (streamsWithEntries == 0) {
            response = new StringBuilder("*0\r\n");
        }

        client.write(ByteBuffer.wrap(response.toString().getBytes()));
    }

    @Override
    public void execute(SocketChannel client, List<String> commandParts) throws Exception {
        throw new Exception("ERR wrong context for 'XREAD' command");
    }

    @Override
    public void execute(SocketChannel client) throws Exception {
        throw new Exception("ERR wrong context for 'XREAD' command");
    }
}