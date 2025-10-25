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
#include "RetentionPolicy.h"
#include "Metrics.h"

void demonstrateRetentionPolicy() {
    std::cout << "\n=== Retention Policy Demo ===" << '\n';

    // Set log level to see retention activities
    Metrics::getInstance().setLogLevel(LogLevel::INFO);

    // Create broker and start services
    Broker broker("retention-broker");
    broker.createTopic("retention-topic", 2);
    
    std::cout << "Starting async writer and retention cleaner..." << '\n';
    broker.startAsyncWriter();
    broker.startRetentionCleaner();
    
    // Give services time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create producer
    Producer producer(broker);

    // Send messages with different timestamps
    std::cout << "\nSending 20 messages..." << '\n';
    for (int i = 0; i < 20; ++i) {
        producer.send("retention-topic", "key" + std::to_string(i), "message" + std::to_string(i));
        std::this_thread::sleep_for(std::chrono::milliseconds(50)); // Small delay
    }

    std::cout << "Messages sent. Waiting for processing..." << '\n';
    
    // Wait for async writer to process all messages
    while (broker.getAsyncQueueSize("retention-topic") > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "All messages processed." << '\n';
    
    // Show initial statistics
    std::cout << "\nInitial Statistics:" << '\n';
    Metrics::getInstance().printStatistics();
    
    std::cout << "\nWaiting for retention cleaner to run..." << '\n';
    std::this_thread::sleep_for(std::chrono::seconds(15));
    
    // Show final statistics
    std::cout << "\nFinal Statistics:" << '\n';
    Metrics::getInstance().printStatistics();
    
    std::cout << "Retention cleaner stats:" << '\n';
    std::cout << "  Total cleaned messages: " << broker.getTotalCleanedMessages() << '\n';
    std::cout << "  Total cleaned bytes: " << broker.getTotalCleanedBytes() << '\n';
    
    // Stop services
    broker.stopRetentionCleaner();
    broker.stopAsyncWriter();
    
    std::cout << "Retention policy demo completed!" << '\n';
}

void demonstrateRetentionPolicySettings() {
    std::cout << "\n=== Retention Policy Settings Demo ===" << '\n';

    // Test different retention policies
    std::cout << "\nTesting different retention policies:" << '\n';
    
    // 1. Time-based retention (1 hour)
    RetentionPolicy timeBased(std::chrono::hours(1), 0);
    std::cout << "1. Time-based (1 hour): " << timeBased.toString() << '\n';
    
    // 2. Size-based retention (1MB)
    RetentionPolicy sizeBased(std::chrono::hours(0), 1024 * 1024);
    std::cout << "2. Size-based (1MB): " << sizeBased.toString() << '\n';
    
    // 3. Combined retention (7 days, 100MB)
    RetentionPolicy combined(std::chrono::hours(24 * 7), 100 * 1024 * 1024);
    std::cout << "3. Combined (7 days, 100MB): " << combined.toString() << '\n';
    
    // 4. No retention (unlimited)
    RetentionPolicy unlimited(std::chrono::hours(0), 0);
    std::cout << "4. Unlimited: " << unlimited.toString() << '\n';
    
    // Test message retention logic
    std::cout << "\nTesting message retention logic:" << '\n';
    
    auto now = std::chrono::system_clock::now();
    auto oldTime = now - std::chrono::hours(2);  // 2 hours ago
    auto recentTime = now - std::chrono::minutes(30);  // 30 minutes ago
    
    std::cout << "Old message (2 hours ago) with 1-hour retention: " 
              << (timeBased.shouldRetain(oldTime, 1000) ? "RETAINED" : "DELETED") << '\n';
    std::cout << "Recent message (30 min ago) with 1-hour retention: " 
              << (timeBased.shouldRetain(recentTime, 1000) ? "RETAINED" : "DELETED") << '\n';
    
    std::cout << "Small message (1KB) with 1MB size limit: " 
              << (sizeBased.shouldRetain(now, 1024) ? "RETAINED" : "DELETED") << '\n';
    std::cout << "Large message (2MB) with 1MB size limit: " 
              << (sizeBased.shouldRetain(now, 2 * 1024 * 1024) ? "RETAINED" : "DELETED") << '\n';
    
    std::cout << "Retention policy settings demo completed!" << '\n';
}

void demonstrateRetentionCleanerPerformance() {
    std::cout << "\n=== Retention Cleaner Performance Demo ===" << '\n';

    Broker broker("perf-broker");
    broker.createTopic("perf-topic", 1);
    
    broker.startAsyncWriter();
    broker.startRetentionCleaner();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    Producer producer(broker);

    // Send many messages quickly
    std::cout << "Sending 1000 messages quickly..." << '\n';
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 1000; ++i) {
        producer.send("perf-topic", "key" + std::to_string(i), "message" + std::to_string(i));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Sent 1000 messages in " << duration.count() << "ms" << '\n';
    
    // Wait for processing
    while (broker.getAsyncQueueSize("perf-topic") > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    std::cout << "All messages processed." << '\n';
    
    // Show performance statistics
    std::cout << "\nPerformance Statistics:" << '\n';
    Metrics::getInstance().printStatistics();
    
    std::cout << "Retention cleaner performance:" << '\n';
    std::cout << "  Total cleaned messages: " << broker.getTotalCleanedMessages() << '\n';
    std::cout << "  Total cleaned bytes: " << broker.getTotalCleanedBytes() << '\n';
    
    broker.stopRetentionCleaner();
    broker.stopAsyncWriter();
    
    std::cout << "Retention cleaner performance demo completed!" << '\n';
}

int main() {
    try {
        demonstrateRetentionPolicySettings();
        demonstrateRetentionPolicy();
        demonstrateRetentionCleanerPerformance();
        
        std::cout << "\n=== All retention demos completed successfully! ===" << '\n';

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
