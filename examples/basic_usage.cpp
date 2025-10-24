#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

// Include our Kafka components
#include "Message.h"
#include "Partition.h"
#include "Topic.h"
#include "Broker.h"
#include "Producer.h"
#include "Consumer.h"

void demonstrateBasicUsage() {
    std::cout << "=== SelfKafka Basic Usage Demo ===" << '\n';
    
    // 1. Create broker and topics
    std::cout << "\n1. Creating broker and topics..." << '\n';
    Broker broker("main-broker");
    broker.createTopic("user-events", 3);  // 3 partitions
    broker.createTopic("orders", 2);       // 2 partitions
    
    std::cout << "Created topics: ";
    auto topics = broker.listTopics();
    for (const auto& topic : topics) {
        std::cout << topic << " ";
    }
    std::cout << '\n';
    
    // 2. Create producer and send messages
    std::cout << "\n2. Sending messages..." << '\n';
    Producer producer(broker);
    
    // Send messages to user-events topic
    producer.send("user-events", "user123", "login");
    producer.send("user-events", "user456", "logout");
    producer.send("user-events", "user123", "purchase");
    producer.send("user-events", "user789", "login");
    
    // Send messages to orders topic
    producer.send("orders", "order001", "created");
    producer.send("orders", "order002", "shipped");
    
    std::cout << "Sent 6 messages total" << '\n';
    
    // 3. Create consumer and read messages
    std::cout << "\n3. Reading messages..." << '\n';
    Consumer consumer(broker, "user-events");
    
    // Read messages from partition 0
    std::cout << "Messages from partition 0:" << '\n';
    try {
        for (int i = 0; i < 5; ++i) {  // Try to read 5 messages
            Message msg = consumer.poll(0);
            std::cout << "  " << msg.toString() << '\n';
        }
    } catch (const std::runtime_error& e) {
        std::cout << "  No more messages: " << e.what() << '\n';
    }
    
    // 4. Show topic statistics
    std::cout << "\n4. Topic statistics:" << '\n';
    std::cout << "user-events topic size: " << broker.getMessages("user-events", 0, 0, 10).size() << " messages" << '\n';
    std::cout << "orders topic size: " << broker.getMessages("orders", 0, 0, 10).size() << " messages" << '\n';
}

void demonstrateMultiThreading() {
    std::cout << "\n=== Multi-threading Demo ===" << '\n';
    
    // Create broker and topic
    Broker broker("thread-broker");
    broker.createTopic("events", 2);
    
    // Producer thread
    std::thread producerThread([&broker]() {
        Producer producer(broker);
        for (int i = 0; i < 10; ++i) {
            producer.send("events", "thread-" + std::to_string(i % 3), "message-" + std::to_string(i));
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::cout << "Producer finished sending 10 messages" << '\n';
    });
    
    // Consumer thread
    std::thread consumerThread([&broker]() {
        Consumer consumer(broker, "events");
        int messagesRead = 0;
        
        while (messagesRead < 10) {
            bool foundMessage = false;
            // Try to read from all partitions
            for (uint32_t partitionId = 0; partitionId < 2; ++partitionId) {
                try {
                    Message msg = consumer.poll(partitionId);
                    std::cout << "Consumer read: " << msg.toString() << '\n';
                    messagesRead++;
                    foundMessage = true;
                    break; // Found a message, exit partition loop
                } catch (const std::runtime_error&) {
                    // No message in this partition, try next
                }
            }
            
            if (!foundMessage) {
                // No messages in any partition, wait a bit
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        std::cout << "Consumer finished reading 10 messages" << '\n';
    });
    
    // Wait for threads to complete
    producerThread.join();
    consumerThread.join();
}

void demonstratePartitionRouting() {
    std::cout << "\n=== Partition Routing Demo ===" << '\n';
    
    Broker broker("routing-broker");
    broker.createTopic("routing-test", 3);  // 3 partitions
    
    Producer producer(broker);
    
    // Send messages with different keys to see routing
    std::vector<std::string> keys = {"user123", "user456", "user789", "user123", "user456"};
    
    std::cout << "Sending messages with different keys:" << '\n';
    for (size_t i = 0; i < keys.size(); ++i) {
        producer.send("routing-test", keys[i], "message-" + std::to_string(i));
        std::cout << "  Sent: key=" << keys[i] << " -> partition=" << (std::hash<std::string>{}(keys[i]) % 3) << '\n';
    }
    
    // Show messages in each partition
    std::cout << "\nMessages by partition:" << '\n';
    for (uint32_t partitionId = 0; partitionId < 3; ++partitionId) {
        auto messages = broker.getMessages("routing-test", partitionId, 0, 10);
        std::cout << "Partition " << partitionId << ": " << messages.size() << " messages" << '\n';
        for (const auto& msg : messages) {
            std::cout << "  " << msg.toString() << '\n';
        }
    }
}

void demonstrateMetadataAPI() {
    std::cout << "\n=== Metadata API Demo ===" << '\n';
    
    // Create broker and topics with some data
    Broker broker("metadata-broker");
    broker.createTopic("user-events", 3);
    broker.createTopic("orders", 2);
    
    Producer producer(broker);
    
    // Send some messages to different topics
    producer.send("user-events", "user123", "login");
    producer.send("user-events", "user456", "logout");
    producer.send("user-events", "user789", "purchase");
    producer.send("orders", "order001", "created");
    producer.send("orders", "order002", "shipped");
    
    // Get metadata for all topics
    std::cout << "=== All Topics Metadata ===" << '\n';
    auto topicsMetadata = broker.getTopicsMetadata();
    for (const auto& topic : topicsMetadata) {
        std::cout << "Topic: " << topic.name 
                  << ", Partitions: " << topic.numPartitions
                  << ", Total Messages: " << topic.totalMessages << '\n';
        
        for (const auto& partition : topic.partitions) {
            std::cout << "  Partition " << partition.id 
                      << ": " << partition.messageCount << " messages"
                      << ", Offsets: " << partition.firstOffset << "-" << partition.lastOffset << '\n';
        }
    }
    
    // Get metadata for specific topic
    std::cout << "\n=== User Events Topic Metadata ===" << '\n';
    auto partitionMetadata = broker.getPartitionMetadata("user-events");
    for (const auto& partition : partitionMetadata) {
        std::cout << "Partition " << partition.id 
                  << ": " << partition.messageCount << " messages"
                  << ", Offsets: " << partition.firstOffset << "-" << partition.lastOffset << '\n';
    }
}

int main() {
    try {
        demonstrateBasicUsage();
        demonstrateMultiThreading();
        demonstratePartitionRouting();
        demonstrateMetadataAPI();
        
        std::cout << "\n=== Demo completed successfully! ===" << '\n';
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }
    
    return 0;
}
