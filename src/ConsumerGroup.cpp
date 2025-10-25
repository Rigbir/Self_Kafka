#include "ConsumerGroup.h"

ConsumerGroup::ConsumerGroup(const std::string& groupId, Broker& broker, const std::string& topicName):
        groupId_(groupId),
        broker_(broker),
        topicName_(topicName),
        running_(true),
        connection_(nullptr),
        connectionString_("dbname=selfkafka user=" + std::string(getenv("USER") ? getenv("USER") : "postgres")) {
    
    
    connection_ = PQconnectdb(connectionString_.c_str());
    if (PQstatus(connection_) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(connection_) << std::endl;
        PQfinish(connection_);
        connection_ = nullptr;
    } else {
        std::cout << "Connected to PostgreSQL database" << std::endl;
    }
    
    loadFromDatabase();
}

ConsumerGroup::~ConsumerGroup() {
    stop();
    if (connection_) {
        PQfinish(connection_);
    }
}

void ConsumerGroup::addConsumer(std::shared_ptr<Consumer> consumer) {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::string consumerId = "consumer-" + std::to_string(timestamp) + "-" + std::to_string(consumers_.size());
    
    consumers_.push_back(consumer);
    consumersMap_[consumerId] = consumer;
    
    lastHeartbeats_[consumerId] = now;
    
    rebalance();
    saveToDatabase();
}

void ConsumerGroup::removeConsumer(std::shared_ptr<Consumer> consumer) {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    
    auto it = std::find(consumers_.begin(), consumers_.end(), consumer);
    if (it != consumers_.end()) {
        consumers_.erase(it);
    }
    
    for (auto mapIt = consumersMap_.begin(); mapIt != consumersMap_.end(); ++mapIt) {
        if (mapIt->second == consumer) {
            std::string consumerId = mapIt->first;
            consumersMap_.erase(mapIt);
            lastHeartbeats_.erase(consumerId);
            break;
        }
    }
    
    for (auto assignIt = partitionAssignments_.begin(); assignIt != partitionAssignments_.end();) {
        if (assignIt->second == consumer) {
            assignIt = partitionAssignments_.erase(assignIt);
        } else {
            ++assignIt;
        }
    }
    
    rebalance();
    saveToDatabase();
}

void ConsumerGroup::sendHeartbeat(const std::string& consumerId) {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    lastHeartbeats_[consumerId] = std::chrono::system_clock::now();
}

// Lifecycle: Starts the heartbeat monitoring thread
void ConsumerGroup::start() {
    if (running_.load()) {
        return;  
    }
    
    running_.store(true);
    heartbeatMonitorThread_ = std::thread(&ConsumerGroup::heartbeatMonitor, this);
}

// Lifecycle: Stops the heartbeat monitoring thread
void ConsumerGroup::stop() {
    if (!running_.load()) {
        return;  
    }
    
    running_.store(false);
    if (heartbeatMonitorThread_.joinable()) {
        heartbeatMonitorThread_.join();
    }
}

std::vector<uint32_t> ConsumerGroup::getAssignedPartitions(const std::string& consumerId) const {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    std::vector<uint32_t> assignedPartitions;
    
    auto consumerIt = consumersMap_.find(consumerId);
    if (consumerIt == consumersMap_.end()) {
        return assignedPartitions; 
    }
    
    auto consumer = consumerIt->second;
    
    for (const auto& [partitionId, assignedConsumer] : partitionAssignments_) {
        if (assignedConsumer == consumer) {
            assignedPartitions.push_back(partitionId);
        }
    }
    
    return assignedPartitions;
}

size_t ConsumerGroup::getConsumerCount() const {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    return consumers_.size();
}

std::string ConsumerGroup::getGroupId() const {
    return groupId_;
}

std::vector<std::string> ConsumerGroup::getActiveConsumers() const {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    std::vector<std::string> activeConsumers;
    
    for (const auto& [consumerId, lastHeartbeat] : lastHeartbeats_) {
        auto now = std::chrono::system_clock::now();
        auto timeSinceLastHeartbeat = now - lastHeartbeat;
        if (timeSinceLastHeartbeat < heartbeatTimeout_) {
            activeConsumers.push_back(consumerId);
        }
    }
    
    return activeConsumers;
}

bool ConsumerGroup::isConsumerActive(const std::string& consumerId) const {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    auto it = lastHeartbeats_.find(consumerId);
    if (it == lastHeartbeats_.end()) {
        return false;  
    }
    
    auto now = std::chrono::system_clock::now();
    auto timeSinceLastHeartbeat = now - it->second;
    return timeSinceLastHeartbeat < heartbeatTimeout_;
}

void ConsumerGroup::rebalance() {
    partitionAssignments_.clear();
    
    if (consumers_.empty()) {
        return;  
    }
    
    // Get number of partitions for this topic
    // Note: We need to add a method to Broker to get partition count
    // For now, let's assume we know the partition count
    size_t numPartitions = 3;  // TODO: Get from broker
    
    // Round-robin assignment: distribute partitions evenly among consumers
    for (size_t partitionId = 0; partitionId < numPartitions; ++partitionId) {
        size_t consumerIndex = partitionId % consumers_.size();
        partitionAssignments_[static_cast<uint32_t>(partitionId)] = consumers_[consumerIndex];
    }
}

// Background: Monitors consumer heartbeats and removes inactive consumers
void ConsumerGroup::heartbeatMonitor() {
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        
        if (!running_.load()) break;
        
        std::lock_guard<std::mutex> lock(consumersMutex_);
        auto now = std::chrono::system_clock::now();
        
        std::vector<std::shared_ptr<Consumer>> deadConsumers;
        
        for (auto it = lastHeartbeats_.begin(); it != lastHeartbeats_.end();) {
            auto timeSinceLastHeartbeat = now - it->second;
            if (timeSinceLastHeartbeat >= heartbeatTimeout_) {
                auto consumerIt = consumersMap_.find(it->first);
                if (consumerIt != consumersMap_.end()) {
                    deadConsumers.push_back(consumerIt->second);
                }
                it = lastHeartbeats_.erase(it);
            } else {
                ++it;
            }
        }
        
        for (auto& deadConsumer : deadConsumers) {
            removeConsumer(deadConsumer);
        }
    }
}

// Persistence: Saves consumer group state to database
void ConsumerGroup::saveToDatabase() {
    if (!connection_) {
        std::cerr << "No database connection available" << std::endl;
        return;
    }
    
    
    PGresult* result = PQexec(connection_, "BEGIN");
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        std::cerr << "BEGIN command failed: " << PQerrorMessage(connection_) << std::endl;
        PQclear(result);
        return;
    }
    PQclear(result);
    
    std::string insertGroup = "INSERT INTO consumer_groups (group_id, topic_name) VALUES ($1, $2) "
                             "ON CONFLICT (group_id) DO UPDATE SET topic_name = $2, updated_at = CURRENT_TIMESTAMP";
    const char* values[2] = {groupId_.c_str(), topicName_.c_str()};
    result = PQexecParams(connection_, insertGroup.c_str(), 2, nullptr, values, nullptr, nullptr, 0);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        std::cerr << "Insert consumer group failed: " << PQerrorMessage(connection_) << std::endl;
        PQclear(result);
        PQexec(connection_, "ROLLBACK");
        return;
    }
    PQclear(result);
    
    std::string deleteConsumers = "DELETE FROM consumers WHERE group_id = $1";
    const char* deleteValues[1] = {groupId_.c_str()};
    result = PQexecParams(connection_, deleteConsumers.c_str(), 1, nullptr, deleteValues, nullptr, nullptr, 0);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        std::cerr << "Delete consumers failed: " << PQerrorMessage(connection_) << std::endl;
        PQclear(result);
        PQexec(connection_, "ROLLBACK");
        return;
    }
    PQclear(result);
    
    for (const auto& [consumerId, consumer] : consumersMap_) {
        auto lastHeartbeat = lastHeartbeats_.find(consumerId);
        if (lastHeartbeat != lastHeartbeats_.end()) {
            std::string insertConsumer = "INSERT INTO consumers (consumer_id, group_id, last_heartbeat) VALUES ($1, $2, to_timestamp($3))";
            auto heartbeatTime = std::chrono::system_clock::to_time_t(lastHeartbeat->second);
            std::string heartbeatStr = std::to_string(heartbeatTime);
            const char* consumerValues[3] = {consumerId.c_str(), groupId_.c_str(), heartbeatStr.c_str()};
            
            result = PQexecParams(connection_, insertConsumer.c_str(), 3, nullptr, consumerValues, nullptr, nullptr, 0);
            if (PQresultStatus(result) != PGRES_COMMAND_OK) {
                std::cerr << "Insert consumer failed: " << PQerrorMessage(connection_) << std::endl;
                PQclear(result);
                PQexec(connection_, "ROLLBACK");
                return;
            }
            PQclear(result);
            
            for (const auto& [partitionId, assignedConsumer] : partitionAssignments_) {
                if (assignedConsumer == consumer) {
                    std::string insertAssignment = "INSERT INTO partition_assignments (group_id, consumer_id, partition_id) VALUES ($1, $2, $3)";
                    std::string partitionStr = std::to_string(partitionId);
                    const char* assignmentValues[3] = {groupId_.c_str(), consumerId.c_str(), partitionStr.c_str()};
                    
                    result = PQexecParams(connection_, insertAssignment.c_str(), 3, nullptr, assignmentValues, nullptr, nullptr, 0);
                    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
                        std::cerr << "Insert assignment failed: " << PQerrorMessage(connection_) << std::endl;
                        PQclear(result);
                        PQexec(connection_, "ROLLBACK");
                        return;
                    }
                    PQclear(result);
                }
            }
        }
    }
    
    result = PQexec(connection_, "COMMIT");
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        std::cerr << "COMMIT command failed: " << PQerrorMessage(connection_) << std::endl;
        PQclear(result);
        return;
    }
    PQclear(result);
    
    std::cout << "Saved ConsumerGroup " << groupId_ << " to PostgreSQL" << std::endl;
}

// Persistence: Loads consumer group state from database
void ConsumerGroup::loadFromDatabase() {
    if (!connection_) {
        std::cout << "No database connection available for loading" << std::endl;
        return;
    }
    
    std::string checkGroup = "SELECT group_id FROM consumer_groups WHERE group_id = $1";
    const char* checkValues[1] = {groupId_.c_str()};
    PGresult* result = PQexecParams(connection_, checkGroup.c_str(), 1, nullptr, checkValues, nullptr, nullptr, 0);
    
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        std::cerr << "Check group failed: " << PQerrorMessage(connection_) << std::endl;
        PQclear(result);
        return;
    }
    
    if (PQntuples(result) == 0) {
        PQclear(result);
        std::string insertGroup = "INSERT INTO consumer_groups (group_id, topic_name) VALUES ($1, $2)";
        const char* insertValues[2] = {groupId_.c_str(), topicName_.c_str()};
        result = PQexecParams(connection_, insertGroup.c_str(), 2, nullptr, insertValues, nullptr, nullptr, 0);
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
            std::cerr << "Insert group failed: " << PQerrorMessage(connection_) << std::endl;
        }
        PQclear(result);
        std::cout << "Created new ConsumerGroup " << groupId_ << " in PostgreSQL" << std::endl;
        return;
    }
    PQclear(result);
    
    std::string loadConsumers = "SELECT c.consumer_id, c.last_heartbeat, pa.partition_id "
                               "FROM consumers c "
                               "LEFT JOIN partition_assignments pa ON c.consumer_id = pa.consumer_id "
                               "WHERE c.group_id = $1";
    const char* loadValues[1] = {groupId_.c_str()};
    result = PQexecParams(connection_, loadConsumers.c_str(), 1, nullptr, loadValues, nullptr, nullptr, 0);
    
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        std::cerr << "Load consumers failed: " << PQerrorMessage(connection_) << std::endl;
        PQclear(result);
        return;
    }
    
    int numRows = PQntuples(result);
    std::cout << "Loaded " << numRows << " consumer records from PostgreSQL" << std::endl;
    
    for (int i = 0; i < numRows; ++i) {
        std::string consumerId = PQgetvalue(result, i, 0);
        std::string heartbeatStr = PQgetvalue(result, i, 1);
        
        if (!heartbeatStr.empty()) {
            time_t heartbeatTime = std::stol(heartbeatStr);
            auto heartbeat = std::chrono::system_clock::from_time_t(heartbeatTime);
            lastHeartbeats_[consumerId] = heartbeat;
        }
    }
    
    PQclear(result);
    std::cout << "Loaded ConsumerGroup " << groupId_ << " from PostgreSQL" << std::endl;
}