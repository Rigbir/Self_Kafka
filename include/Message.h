#pragma once

#include <string>
#include <chrono>
#include <iomanip>
#include <cstdint>
#include <sstream>

class Message {
public:
    Message(std::string key, std::string value);
    Message(std::string key, std::string value, uint64_t offset);
    Message(std::string key, std::string value, uint64_t offset, std::chrono::system_clock::time_point timestamp);

    uint64_t getOffset() const;
    const std::string& getKey() const;
    const std::string& getValue() const;
    std::chrono::system_clock::time_point getTimestamp() const;

    std::string toString() const; // For debugging purposes

private:
    uint64_t offset_;
    std::string key_;
    std::string value_;
    std::chrono::system_clock::time_point timestamp_;
};