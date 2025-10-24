#pragma once

#include "Broker.h"

#include <string>

class Producer {
public:
    explicit Producer(Broker& broker);

    void send(const std::string& topicName, const std::string& key, const std::string& value);

private:
    Broker& broker_;
};