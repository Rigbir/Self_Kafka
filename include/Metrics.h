#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <unordered_map>
#include <mutex>

// Log levels
enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3
};

class Metrics {
public:
    static Metrics& getInstance();
    
    // Message counters
    void incrementMessagesSent();
    void incrementMessagesReceived();
    void incrementMessagesProcessed();
    void incrementMessagesDropped();
    
    // Queue metrics
    void updateQueueSize(const std::string& topicName, size_t size);
    void recordProcessingTime(const std::string& topicName, std::chrono::milliseconds time);
    
    // Getters
    uint64_t getMessagesSent() const;
    uint64_t getMessagesReceived() const;
    uint64_t getMessagesProcessed() const;
    uint64_t getMessagesDropped() const;
    
    size_t getQueueSize(const std::string& topicName) const;
    double getAverageProcessingTime(const std::string& topicName) const;
    
    // Logging
    void setLogLevel(LogLevel level);
    void log(LogLevel level, const std::string& message);
    void logInfo(const std::string& message);
    void logWarn(const std::string& message);
    void logError(const std::string& message);
    void logDebug(const std::string& message);
    
    // Statistics
    void printStatistics() const;
    void reset();

private:
    Metrics() = default;
    
    // Message counters
    std::atomic<uint64_t> messagesSent_{0};
    std::atomic<uint64_t> messagesReceived_{0};
    std::atomic<uint64_t> messagesProcessed_{0};
    std::atomic<uint64_t> messagesDropped_{0};
    
    // Queue metrics
    std::unordered_map<std::string, std::atomic<size_t>> queueSizes_;
    std::unordered_map<std::string, std::atomic<double>> totalProcessingTime_;
    std::unordered_map<std::string, std::atomic<uint64_t>> processingCount_;
    mutable std::mutex queueMetricsMutex_;
    
    // Logging
    std::atomic<LogLevel> logLevel_{LogLevel::INFO};
    
    // Helper methods
    std::string getCurrentTime() const;
    std::string logLevelToString(LogLevel level) const;
};
