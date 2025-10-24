#include "Topic.h"

// Constructor: Creates a topic with specified name and number of partitions
Topic::Topic(std::string name, size_t numPartitions):
       name_(std::move(name)),
       numPartitions_(numPartitions) {
    for (size_t i = 0; i < numPartitions_; i++) {
        partitions_.push_back(std::make_shared<Partition>(i));
    }
}

// Core: Routes a message to appropriate partition based on key hash
void Topic::append(const Message& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t partitionId = std::hash<std::string>()(message.getKey()) % numPartitions_;
    partitions_[partitionId]->append(message);
    cv_.notify_all();
}

// Accessor: Returns reference to a specific partition by ID
Partition& Topic::getPartition(uint32_t partitionId) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (partitionId >= numPartitions_) {
        throw std::out_of_range("Partition ID " + std::to_string(partitionId) + " does not exist");
    }

    return *partitions_[partitionId];
}

// Reader: Retrieves all messages from all partitions in this topic
std::vector<Message> Topic::getAllMessages() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Message> allMessages;

    size_t totalSize = 0;
    for (const auto& partition : partitions_) {
        totalSize += partition->size();
    }
    allMessages.reserve(totalSize);

    for (const auto& partition : partitions_) {
        auto partitionMessages = partition->getAllMessages();
        allMessages.insert(allMessages.end(), 
                           std::make_move_iterator(partitionMessages.begin()),
                           std::make_move_iterator(partitionMessages.end()));
    }

    return allMessages;
}

// Utility: Returns total number of messages across all partitions
size_t Topic::size() const {
    std::lock_guard<std::mutex> lock(mutex_);

    size_t totalSize = 0;
    for (const auto& partition : partitions_) {
        totalSize += partition->size();
    }

    return totalSize;
}

// Getter: Returns the name of this topic
std::string Topic::getName() const {
    return name_;
}

// Getter: Returns the number of partitions in this topic
size_t Topic::getNumPartitions() const {
    return numPartitions_;
}