#pragma once

#include "Topic.h"
#include "Message.h"
#include "AsyncWriter.h"

#include <thread>
#include <unordered_map>
#include <memory>

// Forward declaration
class RetentionCleaner;

// Metadata structures for monitoring and administration
struct PartitionMetadata {
    uint32_t id;
    uint64_t messageCount;
    uint64_t firstOffset;
    uint64_t lastOffset;
};

struct TopicMetadata {
    std::string name;
    size_t numPartitions;
    std::vector<PartitionMetadata> partitions;
    uint64_t totalMessages;
};

class Broker {
public:
    explicit Broker(std::string id);
    ~Broker();

    void createTopic(const std::string topicName, size_t numPartitions);
    bool hasTopic(const std::string& topicName) const;
    
    // Async operations (non-blocking)
    void append(const std::string& topicName, const Message& message);
    void send(const std::string& topicName, const std::string& key, const std::string& value);
    
    // Sync operations (for internal use by AsyncWriter)
    void appendSync(const std::string& topicName, const Message& message);

    std::vector<Message> getMessages(const std::string& topicName, uint32_t partitionId, uint64_t from, uint64_t to) const;
    std::vector<std::string> listTopics() const;
    std::string getId() const;
    
    // Async writer management
    void startAsyncWriter();
    void stopAsyncWriter();
    size_t getAsyncQueueSize(const std::string& topicName) const;
    size_t getTotalProcessedMessages() const;
    
    // Retention management
    void startRetentionCleaner();
    void stopRetentionCleaner();
    uint64_t getTotalCleanedMessages() const;
    uint64_t getTotalCleanedBytes() const;
    
    // Metadata API for monitoring and administration
    std::vector<TopicMetadata> getTopicsMetadata() const;
    std::vector<PartitionMetadata> getPartitionMetadata(const std::string& topicName) const;
    
private:
    std::string id_;
    std::unordered_map<std::string, std::shared_ptr<Topic>> topics_; 
    mutable std::mutex mutex_; // Mutex to protect the topics_ map
    
    // Async writer
    std::unique_ptr<AsyncWriter> asyncWriter_;
    
    // Retention cleaner
    std::unique_ptr<RetentionCleaner> retentionCleaner_;

    void checkTopicExists(const std::string& topicName) const;
};