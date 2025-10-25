#pragma once

#include <chrono>
#include <cstdint>

class RetentionPolicy {
public:
    // Constructor with default values
    RetentionPolicy();
    
    // Constructor with custom values
    RetentionPolicy(std::chrono::hours maxAge, uint64_t maxSizeBytes);
    
    // Time-based retention
    void setMaxAge(std::chrono::hours maxAge);
    std::chrono::hours getMaxAge() const;
    
    // Size-based retention
    void setMaxSize(uint64_t maxSizeBytes);
    uint64_t getMaxSize() const;
    
    // Check if message should be retained
    bool shouldRetain(const std::chrono::system_clock::time_point& messageTime, 
                     uint64_t currentSize) const;
    
    // Check if message is too old
    bool isExpired(const std::chrono::system_clock::time_point& messageTime) const;
    
    // Check if size limit exceeded
    bool isSizeExceeded(uint64_t currentSize) const;
    
    // Get retention info as string
    std::string toString() const;

private:
    std::chrono::hours maxAge_;
    uint64_t maxSizeBytes_;
    bool timeBasedRetention_;
    bool sizeBasedRetention_;
};
