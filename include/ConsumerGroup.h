#pragma once

#include "Broker.h"
#include "Consumer.h"

#include <algorithm>
#include <iostream>
#include <libpq-fe.h>

class ConsumerGroup {
public:
    explicit ConsumerGroup(const std::string& groupId, Broker& broker, const std::string& topicName);
    ~ConsumerGroup();

    void addConsumer(std::shared_ptr<Consumer> consumer);
    void removeConsumer(std::shared_ptr<Consumer> consumer);
    void sendHeartbeat(const std::string& consumerId);

    void start();
    void stop();

    std::vector<uint32_t> getAssignedPartitions(const std::string& consumerId) const;
    size_t getConsumerCount() const;
    std::string getGroupId() const;
    std::vector<std::string> getActiveConsumers() const;
    bool isConsumerActive(const std::string& consumerId) const;

    void rebalance();

private:
    void heartbeatMonitor();
    void saveToDatabase();
    void loadFromDatabase();

    // Core data
    std::string groupId_;
    Broker& broker_;
    std::string topicName_;

    // Consumer management
    std::vector<std::shared_ptr<Consumer>> consumers_;
    std::unordered_map<std::string, std::shared_ptr<Consumer>> consumersMap_; 
    std::unordered_map<uint32_t, std::shared_ptr<Consumer>> partitionAssignments_;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> lastHeartbeats_;
    
    // Threads
    std::thread heartbeatMonitorThread_; // Thread to monitor heartbeats
    std::atomic<bool> running_; // Flag to indicate if the consumer group is running
    mutable std::mutex consumersMutex_; // Mutex to protect the consumers_ vector

    // Configuration
    std::chrono::seconds heartbeatTimeout_{90};
    
    // PostgreSQL connection
    PGconn* connection_;
    std::string connectionString_;
};