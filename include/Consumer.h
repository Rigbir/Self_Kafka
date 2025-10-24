#pragma once

#include "Broker.h"
#include "Message.h"

#include <chrono>
#include <mutex>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <condition_variable>

class Consumer {
public:
    explicit Consumer(Broker& broker, const std::string& topicName);

    Message poll(uint32_t partitionId);
    void waitForMessage(uint32_t partitionId);

    void commit(uint32_t partitionId, uint64_t offset);
    uint64_t position(uint32_t partitionId) const;
    void reset(uint32_t partitionId);

private:
    Broker& broker_;
    std::string topicName_;
    std::unordered_map<uint32_t, uint64_t> offsets_;
    mutable std::mutex mutex_;   // Mutex to protect the offsets_ map
    mutable std::condition_variable cv_; // Condition variable to notify when a new message is available
};