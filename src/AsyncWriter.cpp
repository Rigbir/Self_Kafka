#include "AsyncWriter.h"
#include "Broker.h"
#include "Metrics.h"

#include <iostream>
#include <chrono>

// Constructor: Initializes the async writer
AsyncWriter::AsyncWriter(Broker& broker) : 
    broker_(broker), 
    running_(false), 
    totalProcessedMessages_(0) {}

// Destructor: Stops the writer thread
AsyncWriter::~AsyncWriter() {
    stop();
    join();
}

// Lifecycle: Starts the background writer thread
void AsyncWriter::start() {
    if (running_.load()) {
        return;
    }
    
    running_.store(true);
    writerThread_ = std::thread(&AsyncWriter::writerThread, this);
    std::cout << "AsyncWriter started" << std::endl;
}

// Lifecycle: Stops the background writer thread
void AsyncWriter::stop() {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    
    // Shutdown all queues to wake up waiting threads
    std::lock_guard<std::mutex> lock(queuesMutex_);
    for (auto& [topicName, queue] : topicQueues_) {
        queue->shutdown();
    }
    
    std::cout << "AsyncWriter stopping..." << std::endl;
}

// Lifecycle: Waits for the writer thread to finish
void AsyncWriter::join() {
    if (writerThread_.joinable()) {
        writerThread_.join();
        std::cout << "AsyncWriter stopped" << std::endl;
    }
}

// Message handling: Enqueues a message for async processing (copy version)
void AsyncWriter::enqueueMessage(const std::string& topicName, const Message& message) {
    getOrCreateQueue(topicName).push(message);
    Metrics::getInstance().updateQueueSize(topicName, getOrCreateQueue(topicName).size());
}

// Message handling: Enqueues a message for async processing (move version)
void AsyncWriter::enqueueMessage(const std::string& topicName, Message&& message) {
    getOrCreateQueue(topicName).push(std::move(message));
    Metrics::getInstance().updateQueueSize(topicName, getOrCreateQueue(topicName).size());
}

// Statistics: Returns queue size for specific topic
size_t AsyncWriter::getQueueSize(const std::string& topicName) const {
    std::lock_guard<std::mutex> lock(queuesMutex_);
    auto it = topicQueues_.find(topicName);
    return (it != topicQueues_.end()) ? it->second->size() : 0;
}

// Statistics: Returns total number of processed messages
size_t AsyncWriter::getTotalProcessedMessages() const {
    return totalProcessedMessages_.load();
}

// Statistics: Checks if writer is running
bool AsyncWriter::isRunning() const {
    return running_.load();
}

// Background: Main writer thread that processes messages
void AsyncWriter::writerThread() {
    std::cout << "AsyncWriter thread started" << std::endl;
    
    while (running_.load()) {
        bool processedAny = false;
        
        // Process messages from all topic queues
        {
            std::lock_guard<std::mutex> lock(queuesMutex_);
            for (auto& [topicName, queue] : topicQueues_) {
                Message message("", "");
                if (queue->tryPop(message, std::chrono::milliseconds(100))) {
                    try {
                        // Write message to the actual topic
                        broker_.appendSync(topicName, message);
                        totalProcessedMessages_.fetch_add(1);
                        processedAny = true;
                        
                        Metrics::getInstance().updateQueueSize(topicName, queue->size());
                    } catch (const std::exception& e) {
                        Metrics::getInstance().logError("Error writing message to topic " + topicName + ": " + e.what());
                    }
                }
            }
        }
        
        if (!processedAny) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    std::cout << "AsyncWriter thread finished" << std::endl;
}

// Internal: Gets or creates a message queue for a topic
MessageQueue& AsyncWriter::getOrCreateQueue(const std::string& topicName) {
    std::lock_guard<std::mutex> lock(queuesMutex_);
    
    auto it = topicQueues_.find(topicName);
    if (it == topicQueues_.end()) {
        topicQueues_[topicName] = std::make_unique<MessageQueue>();
    }
    
    return *topicQueues_[topicName];
}
