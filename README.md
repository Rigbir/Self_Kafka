# SelfKafka

A minimal Kafka-like message broker implementation in C++ with multithreading support.

## Features

- Core Components: Message, Partition, Topic, Broker, Producer, Consumer
- Multithreading: Thread-safe operations with mutexes and condition variables
- Async Processing: Non-blocking message writing with AsyncWriter
- Metrics & Logging: Performance monitoring and structured logging
- Retention Policies: Automatic cleanup of old messages (time/size-based)
- Consumer Groups: Distributed message consumption with PostgreSQL persistence

Components:
- Producer: Sends messages to topics
- Broker: Manages topics and routes messages
- Topic: Logical grouping of messages
- Partition: Physical storage with ordered messages
- Consumer: Reads messages from partitions
- ConsumerGroup: Coordinates multiple consumers

## Requirements

- C++20 compatible compiler (GCC 10+, Clang 12+, MSVC 2019+)
- CMake 3.10+
- PostgreSQL (for ConsumerGroup persistence)
- libpq (PostgreSQL C library)

## Installation

### macOS
```bash
brew install postgresql libpq
mkdir build && cd build
cmake ..
make
```

## Examples

```bash
./build/basic_usage
./build/async_demo
./build/metrics_demo
./build/retention_demo
```

## Database Setup

```sql
CREATE DATABASE selfkafka;
\i database/schema.sql
```

## Project Structure

```
selfkafka/
├── include/                    # Header files
│   ├── Message.h              # Message data structure
│   ├── Partition.h            # Thread-safe message storage
│   ├── Topic.h                # Topic with multiple partitions
│   ├── Broker.h               # Central message broker
│   ├── Producer.h             # Message producer
│   ├── Consumer.h             # Message consumer
│   ├── AsyncWriter.h          # Asynchronous message writer
│   ├── MessageQueue.h         # Thread-safe message queue
│   ├── Metrics.h              # Performance metrics and logging
│   ├── RetentionPolicy.h      # Message retention policies
│   ├── RetentionCleaner.h     # Background cleanup thread
│   └── ConsumerGroup.h        # Consumer group management
├── src/                       # Implementation files
│   ├── Message.cpp
│   ├── Partition.cpp
│   ├── Topic.cpp
│   ├── Broker.cpp
│   ├── Producer.cpp
│   ├── Consumer.cpp
│   ├── AsyncWriter.cpp
│   ├── MessageQueue.cpp
│   ├── Metrics.cpp
│   ├── RetentionPolicy.cpp
│   ├── RetentionCleaner.cpp
│   └── ConsumerGroup.cpp
├── examples/                  # Demo applications
│   ├── basic_usage.cpp       # Basic producer/consumer demo
│   ├── async_demo.cpp        # Asynchronous processing demo
│   ├── metrics_demo.cpp      # Metrics and logging demo
│   └── retention_demo.cpp    # Retention policy demo
├── database/                  # Database schemas
│   └── schema.sql            # PostgreSQL schema for ConsumerGroups
├── build/                     # Build artifacts (ignored by git)
├── CMakeLists.txt            # CMake build configuration
├── .gitignore               # Git ignore rules
└── README.md                # This file
```

## Troubleshooting

**PostgreSQL Connection Failed**
```bash
brew services start postgresql  # macOS
```

**Build Errors**
```bash
rm -rf build && mkdir build && cd build
cmake .. && make
```
