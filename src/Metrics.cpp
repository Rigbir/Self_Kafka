#include "Metrics.h"
#include <iostream>
#include <iomanip>
#include <sstream>

// Singleton instance
Metrics& Metrics::getInstance() {
    static Metrics instance;
    return instance;
}

// Message counters: Increment sent messages counter
void Metrics::incrementMessagesSent() {
    messagesSent_.fetch_add(1);
    logDebug("Message sent (total: " + std::to_string(messagesSent_.load()) + ")");
}

// Message counters: Increment received messages counter
void Metrics::incrementMessagesReceived() {
    messagesReceived_.fetch_add(1);
    logDebug("Message received (total: " + std::to_string(messagesReceived_.load()) + ")");
}

// Message counters: Increment processed messages counter
void Metrics::incrementMessagesProcessed() {
    messagesProcessed_.fetch_add(1);
    logDebug("Message processed (total: " + std::to_string(messagesProcessed_.load()) + ")");
}

// Message counters: Increment dropped messages counter
void Metrics::incrementMessagesDropped() {
    messagesDropped_.fetch_add(1);
    logWarn("Message dropped (total: " + std::to_string(messagesDropped_.load()) + ")");
}

// Queue metrics: Update queue size for specific topic
void Metrics::updateQueueSize(const std::string& topicName, size_t size) {
    std::lock_guard<std::mutex> lock(queueMetricsMutex_);
    queueSizes_[topicName].store(size);
    logDebug("Queue size updated for topic " + topicName + ": " + std::to_string(size));
}

// Queue metrics: Record processing time for specific topic
void Metrics::recordProcessingTime(const std::string& topicName, std::chrono::milliseconds time) {
    std::lock_guard<std::mutex> lock(queueMetricsMutex_);
    
    double timeMs = static_cast<double>(time.count());
    totalProcessingTime_[topicName].fetch_add(timeMs);
    processingCount_[topicName].fetch_add(1);
    
    logDebug("Processing time for topic " + topicName + ": " + std::to_string(timeMs) + "ms");
}

// Getters: Get total sent messages count
uint64_t Metrics::getMessagesSent() const {
    return messagesSent_.load();
}

// Getters: Get total received messages count
uint64_t Metrics::getMessagesReceived() const {
    return messagesReceived_.load();
}

// Getters: Get total processed messages count
uint64_t Metrics::getMessagesProcessed() const {
    return messagesProcessed_.load();
}

// Getters: Get total dropped messages count
uint64_t Metrics::getMessagesDropped() const {
    return messagesDropped_.load();
}

// Getters: Get queue size for specific topic
size_t Metrics::getQueueSize(const std::string& topicName) const {
    std::lock_guard<std::mutex> lock(queueMetricsMutex_);
    auto it = queueSizes_.find(topicName);
    return (it != queueSizes_.end()) ? it->second.load() : 0;
}

// Getters: Get average processing time for specific topic
double Metrics::getAverageProcessingTime(const std::string& topicName) const {
    std::lock_guard<std::mutex> lock(queueMetricsMutex_);
    
    auto timeIt = totalProcessingTime_.find(topicName);
    auto countIt = processingCount_.find(topicName);
    
    if (timeIt == totalProcessingTime_.end() || countIt == processingCount_.end()) {
        return 0.0;
    }
    
    uint64_t count = countIt->second.load();
    if (count == 0) return 0.0;
    
    return timeIt->second.load() / count;
}

// Logging: Set log level
void Metrics::setLogLevel(LogLevel level) {
    logLevel_.store(level);
    logInfo("Log level set to " + logLevelToString(level));
}

// Logging: Main logging method
void Metrics::log(LogLevel level, const std::string& message) {
    if (level >= logLevel_.load()) {
        std::cout << "[" << getCurrentTime() << "] "
                  << "[" << logLevelToString(level) << "] "
                  << message << std::endl;
    }
}

// Logging: Log info message
void Metrics::logInfo(const std::string& message) {
    log(LogLevel::INFO, message);
}

// Logging: Log warning message
void Metrics::logWarn(const std::string& message) {
    log(LogLevel::WARN, message);
}

// Logging: Log error message
void Metrics::logError(const std::string& message) {
    log(LogLevel::ERROR, message);
}

// Logging: Log debug message
void Metrics::logDebug(const std::string& message) {
    log(LogLevel::DEBUG, message);
}

// Statistics: Print all metrics
void Metrics::printStatistics() const {
    std::cout << "\n=== SelfKafka Metrics ===" << std::endl;
    std::cout << "Messages Sent: " << messagesSent_.load() << std::endl;
    std::cout << "Messages Received: " << messagesReceived_.load() << std::endl;
    std::cout << "Messages Processed: " << messagesProcessed_.load() << std::endl;
    std::cout << "Messages Dropped: " << messagesDropped_.load() << std::endl;
    
    std::lock_guard<std::mutex> lock(queueMetricsMutex_);
    if (!queueSizes_.empty()) {
        std::cout << "\nQueue Sizes:" << std::endl;
        for (const auto& [topicName, size] : queueSizes_) {
            std::cout << "  " << topicName << ": " << size.load() << " messages" << std::endl;
        }
    }
    
    if (!totalProcessingTime_.empty()) {
        std::cout << "\nAverage Processing Times:" << std::endl;
        for (const auto& [topicName, timeAtomic] : totalProcessingTime_) {
            auto countIt = processingCount_.find(topicName);
            if (countIt != processingCount_.end()) {
                uint64_t count = countIt->second.load();
                double avgTime = (count > 0) ? timeAtomic.load() / count : 0.0;
                std::cout << "  " << topicName << ": " << std::fixed << std::setprecision(2) 
                          << avgTime << "ms" << std::endl;
            }
        }
    }
    std::cout << "========================\n" << std::endl;
}

// Statistics: Reset all metrics
void Metrics::reset() {
    messagesSent_.store(0);
    messagesReceived_.store(0);
    messagesProcessed_.store(0);
    messagesDropped_.store(0);
    
    std::lock_guard<std::mutex> lock(queueMetricsMutex_);
    queueSizes_.clear();
    totalProcessingTime_.clear();
    processingCount_.clear();
    
    logInfo("Metrics reset");
}

// Helper: Get current time as string
std::string Metrics::getCurrentTime() const {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

// Helper: Convert log level to string
std::string Metrics::logLevelToString(LogLevel level) const {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
        default: return "UNKNOWN";
    }
}
