#pragma once

#include "RetentionPolicy.h"
#include "Partition.h"
#include "Metrics.h"

#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include <chrono>

class RetentionCleaner {
public:
    RetentionCleaner();
    ~RetentionCleaner();

    // Lifecycle
    void start();
    void stop();
    void join();

    // Partition management
    void addPartition(std::shared_ptr<Partition> partition, const RetentionPolicy& policy);
    void removePartition(std::shared_ptr<Partition> partition);
    void updateRetentionPolicy(std::shared_ptr<Partition> partition, const RetentionPolicy& policy);

    // Statistics
    uint64_t getTotalCleanedMessages() const;
    uint64_t getTotalCleanedBytes() const;
    bool isRunning() const;
    
    // Configuration
    void setCleanupInterval(std::chrono::milliseconds interval);
    std::chrono::milliseconds getCleanupInterval() const;

private:
    void cleanupThread();
    void cleanupPartition(std::shared_ptr<Partition> partition, const RetentionPolicy& policy);
    uint64_t estimateMessageSize(const Message& message) const;

    // Thread management
    std::thread cleanupThread_;
    std::atomic<bool> running_;
    std::chrono::milliseconds cleanupInterval_;

    // Partition tracking
    struct PartitionInfo {
        std::shared_ptr<Partition> partition;
        RetentionPolicy policy;
    };
    
    std::vector<PartitionInfo> partitions_;
    mutable std::mutex partitionsMutex_;

    // Statistics
    std::atomic<uint64_t> totalCleanedMessages_;
    std::atomic<uint64_t> totalCleanedBytes_;
};
