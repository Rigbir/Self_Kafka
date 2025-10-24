#include "Message.h"

// Constructor: Creates a message with auto-assigned offset (0)
Message::Message(std::string key, std::string value):
        offset_(0),
        key_(std::move(key)),
        value_(std::move(value)),
        timestamp_(std::chrono::system_clock::now()) {}

// Constructor: Creates a message with specified offset
Message::Message(std::string key, std::string value, uint64_t offset):
        offset_(offset),
        key_(std::move(key)),
        value_(std::move(value)),
        timestamp_(std::chrono::system_clock::now()) {}

// Constructor: Creates a message with specified offset and timestamp
Message::Message(std::string key, std::string value, uint64_t offset, std::chrono::system_clock::time_point timestamp):
        offset_(offset),
        key_(std::move(key)),
        value_(std::move(value)),
        timestamp_(timestamp) {}

// Getter: Returns the unique offset of this message in the partition
uint64_t Message::getOffset() const {
    return offset_;
}

// Getter: Returns the routing key for this message
const std::string& Message::getKey() const {
    return key_;
}

// Getter: Returns the payload value of this message
const std::string& Message::getValue() const {
    return value_;
}

// Getter: Returns the timestamp when this message was created
std::chrono::system_clock::time_point Message::getTimestamp() const {
    return timestamp_;
}

// Utility: Converts message to string representation for debugging
std::string Message::toString() const {
    std::ostringstream oss;
    std::time_t time = std::chrono::system_clock::to_time_t(timestamp_);
    oss << "[" << offset_ << "] " 
        << "key=" << key_ << ", "
        << "value=" << value_ << ", "
        << "timestamp=" << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S") << "]";
    return oss.str();
}