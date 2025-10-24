#include "Partition.h"

// Constructor: Initializes a partition with given ID
Partition::Partition(uint32_t id):
    id_(id),
    nextOffset_(0) {}

// Core: Appends a message to this partition and assigns proper offset
void Partition::append(const Message& message) {
    // Lock-free atomic operation to get next offset
    uint64_t offset = nextOffset_.fetch_add(1);
    
    std::lock_guard<std::mutex> lock(mutex_);
    Message newMessage(message.getKey(), message.getValue(), offset, message.getTimestamp());
    messages_.push_back(newMessage);
    
    cv_.notify_all();
}

// Core: Blocks until a message with specified offset becomes available
void Partition::waitForMessage(uint64_t offset) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this, offset] {return offset < nextOffset_.load(); }); // Wait until the message is available
}

// Reader: Retrieves a specific message by its offset
const Message Partition::getMessage(uint64_t offset) const {
    std::lock_guard<std::mutex> lock(mutex_);
    checkConsistency();

    if (offset >= nextOffset_.load()) {
        throw std::out_of_range("Offset " + std::to_string(offset) + " does not exist");
    }
    return messages_[offset];
}

// Reader: Retrieves a range of messages from 'from' to 'to' offset
std::vector<Message> Partition::getMessages(uint64_t from, uint64_t to) const {
    std::lock_guard<std::mutex> lock(mutex_);
    checkConsistency();

    if (from >= nextOffset_.load()) return {};
    if (to > nextOffset_.load()) to = nextOffset_.load();

    return std::vector<Message>(messages_.begin() + from, messages_.begin() + to);
}

// Reader: Retrieves all messages in this partition
std::vector<Message> Partition::getAllMessages() const {
    std::lock_guard<std::mutex> lock(mutex_);
    checkConsistency();
    
    return messages_;
}

// Utility: Returns the total number of messages in this partition
uint64_t Partition::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nextOffset_.load();
}

// Getter: Returns the unique ID of this partition
uint32_t Partition::getId() const {
    return id_;
}

// Internal: Validates data consistency between messages_ and nextOffset_
void Partition::checkConsistency() const {
    if (messages_.size() != nextOffset_.load()) {
        throw std::runtime_error("Partition data corruption detected");
    }
}