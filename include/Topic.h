#pragma once

#include "Message.h"
#include "Partition.h"

class Topic {
public:
    explicit Topic(std::string name, size_t numPartitions);

    void append(const Message& message);

    Partition& getPartition(uint32_t partitionId);
    std::vector<Message> getAllMessages();

    size_t size() const;
    std::string getName() const;
    size_t getNumPartitions() const;

private:
    std::string name_;
    std::vector<std::shared_ptr<Partition>> partitions_;
    size_t numPartitions_;
    mutable std::mutex mutex_; // Mutex to protect the partitions_ vector
    mutable std::condition_variable cv_; // Condition variable to notify when a new message is appended
};