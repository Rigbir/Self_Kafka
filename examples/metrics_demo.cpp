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
#include "Metrics.h"

void demonstrateMetrics() {
    std::cout << "\n=== Metrics Demo ===" << '\n';

    // Set log level to see debug messages
    Metrics::getInstance().setLogLevel(LogLevel::INFO);

    // Create broker and start async writer
    Broker broker("metrics-broker");
    broker.createTopic("metrics-topic", 2);
    
    std::cout << "Starting async writer..." << '\n';
    broker.startAsyncWriter();
    
    // Give async writer time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create producer
    Producer producer(broker);

    // Send messages and show metrics
    std::cout << "\nSending 50 messages..." << '\n';
    for (int i = 0; i < 50; ++i) {
        producer.send("metrics-topic", "key" + std::to_string(i), "message" + std::to_string(i));
        
        // Show metrics every 10 messages
        if ((i + 1) % 10 == 0) {
            std::cout << "Sent " << (i + 1) << " messages" << '\n';
            std::cout << "Queue size: " << broker.getAsyncQueueSize("metrics-topic") << '\n';
        }
    }

    // Wait for processing
    std::cout << "\nWaiting for async writer to process messages..." << '\n';
    while (broker.getAsyncQueueSize("metrics-topic") > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::cout << "Queue size: " << broker.getAsyncQueueSize("metrics-topic") << '\n';
    }

    // Show final metrics
    std::cout << "\nFinal Statistics:" << '\n';
    Metrics::getInstance().printStatistics();
    
    // Stop async writer
    broker.stopAsyncWriter();
    
    std::cout << "Metrics demo completed!" << '\n';
}

void demonstrateLogLevels() {
    std::cout << "\n=== Log Levels Demo ===" << '\n';

    Broker broker("log-broker");
    broker.createTopic("log-topic", 1);
    broker.startAsyncWriter();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    Producer producer(broker);

    // Test different log levels
    std::cout << "\nTesting DEBUG level:" << '\n';
    Metrics::getInstance().setLogLevel(LogLevel::DEBUG);
    producer.send("log-topic", "debug", "test");
    
    std::cout << "\nTesting INFO level:" << '\n';
    Metrics::getInstance().setLogLevel(LogLevel::INFO);
    producer.send("log-topic", "info", "test");
    
    std::cout << "\nTesting WARN level:" << '\n';
    Metrics::getInstance().setLogLevel(LogLevel::WARN);
    producer.send("log-topic", "warn", "test");
    
    std::cout << "\nTesting ERROR level:" << '\n';
    Metrics::getInstance().setLogLevel(LogLevel::ERROR);
    producer.send("log-topic", "error", "test");

    // Wait for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    broker.stopAsyncWriter();
    std::cout << "Log levels demo completed!" << '\n';
}

void demonstratePerformanceMetrics() {
    std::cout << "\n=== Performance Metrics Demo ===" << '\n';

    Broker broker("perf-broker");
    broker.createTopic("perf-topic", 3);
    broker.startAsyncWriter();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    Producer producer(broker);

    // Send messages in batches and measure performance
    const int batchSize = 100;
    const int numBatches = 5;
    
    for (int batch = 0; batch < numBatches; ++batch) {
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < batchSize; ++i) {
            producer.send("perf-topic", "batch" + std::to_string(batch), "msg" + std::to_string(i));
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        std::cout << "Batch " << (batch + 1) << ": Sent " << batchSize 
                  << " messages in " << duration.count() << "ms" << '\n';
        
        // Wait for processing
        while (broker.getAsyncQueueSize("perf-topic") > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        std::cout << "Batch " << (batch + 1) << " processed" << '\n';
    }

    // Show performance statistics
    std::cout << "\nPerformance Statistics:" << '\n';
    Metrics::getInstance().printStatistics();
    
    broker.stopAsyncWriter();
    std::cout << "Performance metrics demo completed!" << '\n';
}

int main() {
    try {
        demonstrateMetrics();
        demonstrateLogLevels();
        demonstratePerformanceMetrics();
        
        std::cout << "\n=== All metrics demos completed successfully! ===" << '\n';

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
