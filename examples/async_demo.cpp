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

void demonstrateAsyncWriting() {
    std::cout << "\n=== Async Writing Demo ===" << '\n';

    // Create broker and start async writer
    Broker broker("async-broker");
    broker.createTopic("async-topic", 3);
    
    std::cout << "Starting async writer..." << '\n';
    broker.startAsyncWriter();
    
    // Give async writer time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create producer
    Producer producer(broker);

    // Send messages rapidly (this should be non-blocking)
    std::cout << "Sending 100 messages rapidly..." << '\n';
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 100; ++i) {
        producer.send("async-topic", "key" + std::to_string(i), "message" + std::to_string(i));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Sent 100 messages in " << duration.count() << "ms" << '\n';
    std::cout << "Queue size after sending: " << broker.getAsyncQueueSize("async-topic") << '\n';

    // Wait for async writer to process messages
    std::cout << "Waiting for async writer to process messages..." << '\n';
    while (broker.getAsyncQueueSize("async-topic") > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::cout << "Queue size: " << broker.getAsyncQueueSize("async-topic") << '\n';
    }

    std::cout << "Total processed messages: " << broker.getTotalProcessedMessages() << '\n';
    
    // Verify messages were written
    std::cout << "Topic size: " << broker.getTopicsMetadata()[0].totalMessages << " messages" << '\n';
    
    // Stop async writer
    std::cout << "Stopping async writer..." << '\n';
    broker.stopAsyncWriter();
    
    std::cout << "Async writing demo completed!" << '\n';
}

void demonstrateConcurrentProducers() {
    std::cout << "\n=== Concurrent Producers Demo ===" << '\n';

    Broker broker("concurrent-broker");
    broker.createTopic("concurrent-topic", 2);
    broker.startAsyncWriter();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create multiple producers
    std::vector<std::thread> producerThreads;
    const int numProducers = 5;
    const int messagesPerProducer = 20;

    auto start = std::chrono::high_resolution_clock::now();

    // Start producer threads
    for (int i = 0; i < numProducers; ++i) {
        producerThreads.emplace_back([&broker, i, messagesPerProducer]() {
            Producer producer(broker);
            for (int j = 0; j < messagesPerProducer; ++j) {
                producer.send("concurrent-topic", 
                            "producer" + std::to_string(i), 
                            "message" + std::to_string(j));
            }
        });
    }

    // Wait for all producers to finish
    for (auto& thread : producerThreads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "All producers finished in " << duration.count() << "ms" << '\n';
    std::cout << "Total messages sent: " << (numProducers * messagesPerProducer) << '\n';

    // Wait for processing
    while (broker.getAsyncQueueSize("concurrent-topic") > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "Total processed: " << broker.getTotalProcessedMessages() << '\n';
    std::cout << "Topic size: " << broker.getTopicsMetadata()[0].totalMessages << " messages" << '\n';

    broker.stopAsyncWriter();
    std::cout << "Concurrent producers demo completed!" << '\n';
}

int main() {
    try {
        demonstrateAsyncWriting();
        demonstrateConcurrentProducers();
        
        std::cout << "\n=== All async demos completed successfully! ===" << '\n';

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
