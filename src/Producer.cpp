#include "Producer.h"

// Constructor: Initializes producer with reference to broker
Producer::Producer(Broker& broker):
    broker_(broker) {}

// Core: Sends a message to specified topic (delegates to broker)
void Producer::send(const std::string& topicName, const std::string& key, const std::string& value) {
    broker_.send(topicName, key, value);
}
