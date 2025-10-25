#include "Broker.h"
#include "AsyncWriter.h"
#include "RetentionCleaner.h"
#include "RetentionPolicy.h"
#include "Metrics.h"

// Constructor: Initializes broker with given ID
Broker::Broker(std::string id):
    id_(std::move(id)),
    asyncWriter_(std::make_unique<AsyncWriter>(*this)),
    retentionCleaner_(std::make_unique<RetentionCleaner>()) {}

// Destructor: Stops async writer and retention cleaner
Broker::~Broker() {
    stopAsyncWriter();
    stopRetentionCleaner();
}

// Management: Creates a new topic with specified name and partition count
void Broker::createTopic(const std::string topicName, size_t numPartitions) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (topics_.contains(topicName)) {
        throw std::runtime_error("Topic " + topicName + " already exists");
    }
    
    topics_[topicName] = std::make_shared<Topic>(topicName, numPartitions);
}

// Utility: Checks if a topic with given name exists
bool Broker::hasTopic(const std::string& topicName) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return topics_.contains(topicName);
}

// Core: Appends a message to specified topic (async, non-blocking)
void Broker::append(const std::string& topicName, const Message& message) {
    checkTopicExists(topicName);
    Metrics::getInstance().incrementMessagesSent();
    asyncWriter_->enqueueMessage(topicName, message);
}

// Core: Creates and sends a message to specified topic (async, non-blocking)
void Broker::send(const std::string& topicName, const std::string& key, const std::string& value) {
    checkTopicExists(topicName);
    Message message(key, value);
    Metrics::getInstance().incrementMessagesSent();
    asyncWriter_->enqueueMessage(topicName, std::move(message));
}

// Internal: Synchronous append for use by AsyncWriter
void Broker::appendSync(const std::string& topicName, const Message& message) {
    auto start = std::chrono::high_resolution_clock::now();
    
    std::lock_guard<std::mutex> lock(mutex_);
    checkTopicExists(topicName);
    topics_[topicName]->append(message);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    Metrics::getInstance().incrementMessagesProcessed();
    Metrics::getInstance().recordProcessingTime(topicName, duration);
}

// Reader: Retrieves messages from specific topic and partition
std::vector<Message> Broker::getMessages(const std::string& topicName, uint32_t partitionId, uint64_t from, uint64_t to) const {
    std::lock_guard<std::mutex> lock(mutex_);
    checkTopicExists(topicName);

    Topic& topic = *topics_.at(topicName);
    Partition& partition = topic.getPartition(partitionId);
    return partition.getMessages(from, to);
}

// Utility: Returns list of all topic names managed by this broker
std::vector<std::string> Broker::listTopics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> topics;
    for (const auto& topic : topics_) {
        topics.push_back(topic.first);
    }
    return topics;
}

// Getter: Returns the ID of this broker
std::string Broker::getId() const {
    return id_;
}

// Internal: Validates that specified topic exists, throws if not
void Broker::checkTopicExists(const std::string& topicName) const {
    if (!topics_.contains(topicName)) { 
        throw std::runtime_error("Topic " + topicName + " does not exist");
    }
}

// Metadata: Returns metadata for all topics managed by this broker
std::vector<TopicMetadata> Broker::getTopicsMetadata() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<TopicMetadata> topicsMetadata;
    
    for (const auto& [topicName, topic] : topics_) {
        TopicMetadata topicMeta;
        topicMeta.name = topicName;
        topicMeta.numPartitions = topic->getNumPartitions();
        topicMeta.totalMessages = topic->size();
        
        // Get metadata for each partition
        for (size_t i = 0; i < topicMeta.numPartitions; ++i) {
            PartitionMetadata partitionMeta;
            partitionMeta.id = static_cast<uint32_t>(i);
            partitionMeta.messageCount = topic->getPartition(i).size();
            partitionMeta.firstOffset = (partitionMeta.messageCount > 0) ? 0 : 0;
            partitionMeta.lastOffset = (partitionMeta.messageCount > 0) ? partitionMeta.messageCount - 1 : 0;
            
            topicMeta.partitions.push_back(partitionMeta);
        }
        
        topicsMetadata.push_back(topicMeta);
    }
    
    return topicsMetadata;
}

// Metadata: Returns metadata for partitions of specified topic
std::vector<PartitionMetadata> Broker::getPartitionMetadata(const std::string& topicName) const {
    std::lock_guard<std::mutex> lock(mutex_);
    checkTopicExists(topicName);
    
    std::vector<PartitionMetadata> partitionsMetadata;
    Topic& topic = *topics_.at(topicName);
    
    for (size_t i = 0; i < topic.getNumPartitions(); ++i) {
        PartitionMetadata partitionMeta;
        partitionMeta.id = static_cast<uint32_t>(i);
        partitionMeta.messageCount = topic.getPartition(i).size();
        partitionMeta.firstOffset = (partitionMeta.messageCount > 0) ? 0 : 0;
        partitionMeta.lastOffset = (partitionMeta.messageCount > 0) ? partitionMeta.messageCount - 1 : 0;
        
        partitionsMetadata.push_back(partitionMeta);
    }
    
    return partitionsMetadata;
}

// Async writer management: Starts the async writer
void Broker::startAsyncWriter() {
    asyncWriter_->start();
}

// Async writer management: Stops the async writer
void Broker::stopAsyncWriter() {
    asyncWriter_->stop();
    asyncWriter_->join();
}

// Async writer management: Returns queue size for specific topic
size_t Broker::getAsyncQueueSize(const std::string& topicName) const {
    return asyncWriter_->getQueueSize(topicName);
}

// Async writer management: Returns total processed messages count
size_t Broker::getTotalProcessedMessages() const {
    return asyncWriter_->getTotalProcessedMessages();
}

// Retention management: Starts the retention cleaner
void Broker::startRetentionCleaner() {
    retentionCleaner_->start();
}

// Retention management: Stops the retention cleaner
void Broker::stopRetentionCleaner() {
    retentionCleaner_->stop();
    retentionCleaner_->join();
}

// Retention management: Returns total cleaned messages count
uint64_t Broker::getTotalCleanedMessages() const {
    return retentionCleaner_->getTotalCleanedMessages();
}

// Retention management: Returns total cleaned bytes count
uint64_t Broker::getTotalCleanedBytes() const {
    return retentionCleaner_->getTotalCleanedBytes();
}