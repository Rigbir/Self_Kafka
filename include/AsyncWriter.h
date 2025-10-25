#pragma once

#include "MessageQueue.h"
#include "Topic.h"

#include <thread>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

// Forward declaration
class Broker;

class AsyncWriter {
public:
    explicit AsyncWriter(Broker& broker);
    ~AsyncWriter();

    // Lifecycle
    void start();
    void stop();
    void join();

    // Message handling
    void enqueueMessage(const std::string& topicName, const Message& message);
    void enqueueMessage(const std::string& topicName, Message&& message);

    // Statistics
    size_t getQueueSize(const std::string& topicName) const;
    size_t getTotalProcessedMessages() const;
    bool isRunning() const;

private:
    void writerThread();
    MessageQueue& getOrCreateQueue(const std::string& topicName);

    Broker& broker_;
    std::thread writerThread_;
    std::atomic<bool> running_;
    std::atomic<size_t> totalProcessedMessages_;
    
    // Topic queues
    std::unordered_map<std::string, std::unique_ptr<MessageQueue>> topicQueues_;
    mutable std::mutex queuesMutex_;
};
