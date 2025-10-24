#include "Consumer.h"

// Constructor: Initializes consumer for specified topic
Consumer::Consumer(Broker& broker, const std::string& topicName):
    broker_(broker),
    topicName_(topicName) {}

// Core: Polls for next message from specified partition
Message Consumer::poll(uint32_t partitionId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = offsets_.find(partitionId);
    uint64_t currentOffset = (it != offsets_.end()) ? it->second : 0;

    auto messages = broker_.getMessages(topicName_, partitionId, currentOffset, currentOffset + 1);

    if (!messages.empty()) {
        offsets_[partitionId] = currentOffset + 1;
        return messages[0];
    }

    throw std::runtime_error("No message available");
}

// Core: Blocks until a new message becomes available in specified partition
void Consumer::waitForMessage(uint32_t partitionId) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = offsets_.find(partitionId);
    uint64_t currentOffset = (it != offsets_.end()) ? it->second : 0;

    while (true) {
        bool messageAvailable = cv_.wait_for(lock, std::chrono::milliseconds(100), [this, partitionId, currentOffset] {
            auto messages = broker_.getMessages(topicName_, partitionId, currentOffset, currentOffset + 1);
            return !messages.empty();
        });

        if (messageAvailable) {
            break;
        }
    }
}

// Management: Commits current offset for specified partition
void Consumer::commit(uint32_t partitionId, uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    offsets_[partitionId] = offset;
}

// Getter: Returns current offset position for specified partition
uint64_t Consumer::position(uint32_t partitionId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = offsets_.find(partitionId);
    return (it != offsets_.end()) ? it->second : 0;
}

// Management: Resets offset to beginning (0) for specified partition
void Consumer::reset(uint32_t partitionId) {
    std::lock_guard<std::mutex> lock(mutex_);   
    offsets_[partitionId] = 0;
}