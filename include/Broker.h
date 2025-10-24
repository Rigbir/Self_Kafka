#pragma once

#include "Topic.h"
#include "Message.h"

#include <thread>
#include <unordered_map>

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

    void createTopic(const std::string topicName, size_t numPartitions);
    bool hasTopic(const std::string& topicName) const;
    
    void append(const std::string& topicName, const Message& message);
    void send(const std::string& topicName, const std::string& key, const std::string& value);

    std::vector<Message> getMessages(const std::string& topicName, uint32_t partitionId, uint64_t from, uint64_t to) const;
    std::vector<std::string> listTopics() const;
    std::string getId() const;
    
    // Metadata API for monitoring and administration
    std::vector<TopicMetadata> getTopicsMetadata() const;
    std::vector<PartitionMetadata> getPartitionMetadata(const std::string& topicName) const;
    
private:
    std::string id_;
    std::unordered_map<std::string, std::shared_ptr<Topic>> topics_; 
    mutable std::mutex mutex_; // Mutex to protect the topics_ map

    void checkTopicExists(const std::string& topicName) const;
};