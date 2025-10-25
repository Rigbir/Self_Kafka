#include "RetentionCleaner.h"
#include <iostream>
#include <algorithm>

// Constructor: Initializes the retention cleaner
RetentionCleaner::RetentionCleaner() :
    running_(false),
    cleanupInterval_(std::chrono::seconds(10)),
    totalCleanedMessages_(0),
    totalCleanedBytes_(0) {}

// Destructor: Stops the cleanup thread
RetentionCleaner::~RetentionCleaner() {
    stop();
    join();
}

// Lifecycle: Starts the background cleanup thread
void RetentionCleaner::start() {
    if (running_.load()) {
        return;
    }
    
    running_.store(true);
    cleanupThread_ = std::thread(&RetentionCleaner::cleanupThread, this);
    Metrics::getInstance().logInfo("RetentionCleaner started");
}

// Lifecycle: Stops the background cleanup thread
void RetentionCleaner::stop() {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    Metrics::getInstance().logInfo("RetentionCleaner stopping...");
}

// Lifecycle: Waits for the cleanup thread to finish
void RetentionCleaner::join() {
    if (cleanupThread_.joinable()) {
        cleanupThread_.join();
        Metrics::getInstance().logInfo("RetentionCleaner stopped");
    }
}

// Partition management: Adds a partition for cleanup monitoring
void RetentionCleaner::addPartition(std::shared_ptr<Partition> partition, const RetentionPolicy& policy) {
    std::lock_guard<std::mutex> lock(partitionsMutex_);
    
    PartitionInfo info;
    info.partition = partition;
    info.policy = policy;
    partitions_.push_back(info);
    
    Metrics::getInstance().logInfo("Added partition " + std::to_string(partition->getId()) + 
                                  " to retention cleaner with policy: " + policy.toString());
}

// Partition management: Removes a partition from cleanup monitoring
void RetentionCleaner::removePartition(std::shared_ptr<Partition> partition) {
    std::lock_guard<std::mutex> lock(partitionsMutex_);
    
    auto it = std::find_if(partitions_.begin(), partitions_.end(),
        [partition](const PartitionInfo& info) {
            return info.partition == partition;
        });
    
    if (it != partitions_.end()) {
        partitions_.erase(it);
        Metrics::getInstance().logInfo("Removed partition " + std::to_string(partition->getId()) + 
                                      " from retention cleaner");
    }
}

// Partition management: Updates retention policy for a partition
void RetentionCleaner::updateRetentionPolicy(std::shared_ptr<Partition> partition, const RetentionPolicy& policy) {
    std::lock_guard<std::mutex> lock(partitionsMutex_);
    
    auto it = std::find_if(partitions_.begin(), partitions_.end(),
        [partition](const PartitionInfo& info) {
            return info.partition == partition;
        });
    
    if (it != partitions_.end()) {
        it->policy = policy;
        Metrics::getInstance().logInfo("Updated retention policy for partition " + 
                                      std::to_string(partition->getId()) + ": " + policy.toString());
    }
}

// Statistics: Returns total number of cleaned messages
uint64_t RetentionCleaner::getTotalCleanedMessages() const {
    return totalCleanedMessages_.load();
}

// Statistics: Returns total number of cleaned bytes
uint64_t RetentionCleaner::getTotalCleanedBytes() const {
    return totalCleanedBytes_.load();
}

// Statistics: Checks if cleaner is running
bool RetentionCleaner::isRunning() const {
    return running_.load();
}

// Configuration: Sets cleanup interval
void RetentionCleaner::setCleanupInterval(std::chrono::milliseconds interval) {
    cleanupInterval_ = interval;
    Metrics::getInstance().logInfo("RetentionCleaner interval updated");
}

// Configuration: Gets cleanup interval
std::chrono::milliseconds RetentionCleaner::getCleanupInterval() const {
    return cleanupInterval_;
}

// Background: Main cleanup thread that processes all partitions
void RetentionCleaner::cleanupThread() {
    Metrics::getInstance().logInfo("RetentionCleaner thread started");
    
    while (running_.load()) {
        try {
            std::vector<PartitionInfo> partitionsCopy;
            {
                std::lock_guard<std::mutex> lock(partitionsMutex_);
                partitionsCopy = partitions_;
            }
            
            for (const auto& partitionInfo : partitionsCopy) {
                if (!running_.load()) break;
                cleanupPartition(partitionInfo.partition, partitionInfo.policy);
            }
            
            std::this_thread::sleep_for(cleanupInterval_);
            
        } catch (const std::exception& e) {
            Metrics::getInstance().logError("Error in retention cleanup: " + std::string(e.what()));
            std::this_thread::sleep_for(std::chrono::minutes(1));
        }
    }
    
    Metrics::getInstance().logInfo("RetentionCleaner thread finished");
}

// Internal: Cleans a specific partition based on retention policy
void RetentionCleaner::cleanupPartition(std::shared_ptr<Partition> partition, const RetentionPolicy& policy) {
    try {
        auto allMessages = partition->getAllMessages();
        if (allMessages.empty()) {
            return;
        }
        
        uint64_t currentSize = 0;
        std::vector<Message> messagesToKeep;
        uint64_t cleanedCount = 0;
        uint64_t cleanedBytes = 0;
        
        for (const auto& message : allMessages) {
            uint64_t messageSize = estimateMessageSize(message);
            currentSize += messageSize;
            
            if (policy.shouldRetain(message.getTimestamp(), currentSize)) {
                messagesToKeep.push_back(message);
            } else {
                cleanedCount++;
                cleanedBytes += messageSize;
            }
        }
        
        if (cleanedCount > 0) {
            Metrics::getInstance().logInfo("Cleaned " + std::to_string(cleanedCount) + 
                                          " messages (" + std::to_string(cleanedBytes) + " bytes) " +
                                          "from partition " + std::to_string(partition->getId()));
            
            totalCleanedMessages_.fetch_add(cleanedCount);
            totalCleanedBytes_.fetch_add(cleanedBytes);
        }
        
    } catch (const std::exception& e) {
        Metrics::getInstance().logError("Error cleaning partition " + 
                                       std::to_string(partition->getId()) + ": " + e.what());
    }
}

// Internal: Estimates message size in bytes
uint64_t RetentionCleaner::estimateMessageSize(const Message& message) const {
    return message.getKey().size() + message.getValue().size() + 64;
}
