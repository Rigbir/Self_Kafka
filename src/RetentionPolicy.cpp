#include "RetentionPolicy.h"
#include <sstream>
#include <iomanip>

// Constructor: Initializes with default retention policy (7 days, 1GB)
RetentionPolicy::RetentionPolicy() :
    maxAge_(std::chrono::hours(24 * 7)),  // 7 days
    maxSizeBytes_(1024ULL * 1024 * 1024), // 1GB
    timeBasedRetention_(true),
    sizeBasedRetention_(true) {}

// Constructor: Initializes with custom retention policy
RetentionPolicy::RetentionPolicy(std::chrono::hours maxAge, uint64_t maxSizeBytes) :
    maxAge_(maxAge),
    maxSizeBytes_(maxSizeBytes),
    timeBasedRetention_(maxAge.count() > 0),
    sizeBasedRetention_(maxSizeBytes > 0) {}

// Time-based retention: Set maximum age for messages
void RetentionPolicy::setMaxAge(std::chrono::hours maxAge) {
    maxAge_ = maxAge;
    timeBasedRetention_ = maxAge.count() > 0;
}

// Time-based retention: Get maximum age for messages
std::chrono::hours RetentionPolicy::getMaxAge() const {
    return maxAge_;
}

// Size-based retention: Set maximum size in bytes
void RetentionPolicy::setMaxSize(uint64_t maxSizeBytes) {
    maxSizeBytes_ = maxSizeBytes;
    sizeBasedRetention_ = maxSizeBytes > 0;
}

// Size-based retention: Get maximum size in bytes
uint64_t RetentionPolicy::getMaxSize() const {
    return maxSizeBytes_;
}

// Retention check: Determine if message should be retained based on time and size
bool RetentionPolicy::shouldRetain(const std::chrono::system_clock::time_point& messageTime, 
                                  uint64_t currentSize) const {
    // If time-based retention is enabled and message is expired
    if (timeBasedRetention_ && isExpired(messageTime)) {
        return false;
    }
    
    // If size-based retention is enabled and size limit exceeded
    if (sizeBasedRetention_ && isSizeExceeded(currentSize)) {
        return false;
    }
    
    return true;
}

// Time check: Check if message is too old based on retention policy
bool RetentionPolicy::isExpired(const std::chrono::system_clock::time_point& messageTime) const {
    if (!timeBasedRetention_) {
        return false;
    }
    
    auto now = std::chrono::system_clock::now();
    auto age = now - messageTime;
    return age > maxAge_;
}

// Size check: Check if current size exceeds retention policy limit
bool RetentionPolicy::isSizeExceeded(uint64_t currentSize) const {
    if (!sizeBasedRetention_) {
        return false;
    }
    
    return currentSize > maxSizeBytes_;
}

// Utility: Get retention policy info as string
std::string RetentionPolicy::toString() const {
    std::ostringstream oss;
    oss << "RetentionPolicy(";
    
    if (timeBasedRetention_) {
        oss << "maxAge=" << maxAge_.count() << "h";
    } else {
        oss << "maxAge=unlimited";
    }
    
    oss << ", ";
    
    if (sizeBasedRetention_) {
        if (maxSizeBytes_ >= 1024 * 1024 * 1024) {
            oss << "maxSize=" << std::fixed << std::setprecision(1) 
                << (maxSizeBytes_ / (1024.0 * 1024.0 * 1024.0)) << "GB";
        } else if (maxSizeBytes_ >= 1024 * 1024) {
            oss << "maxSize=" << std::fixed << std::setprecision(1) 
                << (maxSizeBytes_ / (1024.0 * 1024.0)) << "MB";
        } else {
            oss << "maxSize=" << maxSizeBytes_ << "B";
        }
    } else {
        oss << "maxSize=unlimited";
    }
    
    oss << ")";
    return oss.str();
}
