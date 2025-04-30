import config.Config;
import server.RedisServer;
import store.DataStore;

public class Main {
    public static void main(String[] args) {
        try {
            // You can use print statements as follows for debugging, they'll be visible
            // when running tests.
            System.out.println("Logs from your program will appear here!");

            // Initialize configuration, data store, and server
            Config config = new Config(args);
            DataStore store = new DataStore();
            
            // Create and start the Redis server
            RedisServer server = new RedisServer(config, store);
            server.start();
            
        } catch (Exception e) {
            System.out.println("Error starting Redis server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}