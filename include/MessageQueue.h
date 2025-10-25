#pragma once

#include "Message.h"

#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

class MessageQueue {
public:
    MessageQueue();
    ~MessageQueue();

    // Producer operations
    void push(const Message& message);
    void push(Message&& message);
    
    // Consumer operations
    Message pop();
    bool tryPop(Message& message, std::chrono::milliseconds timeout);
    
    // Utility
    size_t size() const;
    bool empty() const;
    void shutdown();

private:
    std::queue<Message> queue_;
    mutable std::mutex mutex_; // Mutex to protect the queue
    std::condition_variable cv_; // Condition variable to notify waiting threads
    std::atomic<bool> shutdown_; // Flag to indicate if the queue is shutting down
};
