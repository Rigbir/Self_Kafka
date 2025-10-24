#pragma once

#include "Message.h"

#include <mutex>
#include <queue>
#include <vector>
#include <atomic>
#include <thread>
#include <stdexcept>
#include <condition_variable>

class Partition {
public:
    explicit Partition(uint32_t id);

    void append(const Message& message);
    void waitForMessage(uint64_t offset);
    const Message getMessage(uint64_t offset) const;
    std::vector<Message> getMessages(uint64_t from, uint64_t to) const;
    std::vector<Message> getAllMessages() const; 

    uint64_t size() const;
    uint32_t getId() const;

private:
    uint32_t id_;
    std::vector<Message> messages_;
    std::atomic<uint64_t> nextOffset_;
    mutable std::mutex mutex_; // Mutex to protect the messages_ vector
    mutable std::condition_variable cv_; // Condition variable to notify when a new message is appended

    void checkConsistency() const;
};