# Java Redis Server Implementation
## Overview
This project is a Java implementation of a Redis-compatible server that supports core Redis functionality including data storage, command processing, pub/sub messaging via streams, replication, and persistence. The server is built using Java NIO for non-blocking I/O operations and efficiently handles multiple client connections in a single threaded environment.
## Features
- **Core Redis Commands**: Supports essential Redis commands like SET, GET, KEYS, TYPE, INCR
- **Data Types**: String values and Streams
- **RESP Protocol**: Full implementation of the Redis serialization protocol
- **Transactions**: Support for MULTI/EXEC/DISCARD commands
- **Persistence**: RDB file-based persistence
- **Replication**: Master-replica setup with full synchronization
- **Streams**: Implementation of Redis Streams including XADD, XRANGE, and XREAD commands
- **Key Expiry**: Automatic key expiration using active expiry cycle (not fully implemented)

## Architecture
The application is structured into several key components:
- **RedisServer**: Main server class handling client connections and command dispatching
- **CommandRegistry**: Registry of available Redis commands and their implementations
- **DataStore**: In-memory storage for Redis data
- **RESPParser**: Parser for the Redis serialization protocol
- **RDBFileParser**: Parser for Redis RDB files
- **ReplicationManager**: Handles master-replica synchronization
- **StreamManager**: Manages Redis Streams functionality
- **TransactionManager**: Manages Redis transactions (MULTI/EXEC/DISCARD)

## Getting Started
### Prerequisites
- Java 24 or higher
- Maven 3.6 or higher

### Installation
Clone the repository:
``` 
   git clone https://github.com/yourusername/redis-java.git
   cd redis-java
```

### Running the Server
Run the server with default configuration:
``` 
./your_program.sh
```
With custom configuration:
``` 
./your_program.sh /path/to/redis.conf
```
### Configuration Options
The server supports the following configuration options:
- `port`: Port to listen on (default: 6379)
- `dir`: Directory for RDB files (default: ".")
- `dbfilename`: Name of the RDB file (default: "dump.rdb")
- `replicaof`: Configure as replica of another Redis instance (format: "host port")

## Connecting to the Server
You can connect to the server using any Redis client (not supported yet):
``` 
redis-cli -p 6379
```
## Supported Commands
- Basic: PING, ECHO
- Keys: SET, GET, DEL, KEYS, TYPE, INCR
- Transactions: MULTI, EXEC, DISCARD
- Streams: XADD, XRANGE, XREAD
- Replication: REPLCONF, PSYNC, WAIT
- Configuration: CONFIG GET, CONFIG SET, INFO

## Replication Setup
To set up a replica server:
Start the master server:
``` 
   ./your_program.sh
```
Start a replica server:
``` 
   ./your_program.sh --port 6380 --replicaof localhost 6379
```
## Project Structure
``` 
.
├── src/main/java/
│   ├── command/       # Redis command implementations
│   ├── config/        # Configuration handling
│   ├── protocol/      # RESP protocol implementation
│   ├── rdb/           # RDB file parsing
│   ├── replication/   # Master-replica functionality
│   ├── server/        # Main server implementation
│   ├── store/         # Data storage
│   ├── streams/       # Redis Streams implementation
│   └── transaction/   # Transaction handling
├── pom.xml            # Maven project configuration
└── README.md          # This file
```


## Notes
- This project is a work in progress
- This code was tested using an external tester ([codecrafters.io](https://app.codecrafters.io/catalog))