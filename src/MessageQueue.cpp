#include "MessageQueue.h"

// Constructor: Initializes the message queue
MessageQueue::MessageQueue() : shutdown_(false) {}

// Destructor: Shuts down the queue
MessageQueue::~MessageQueue() {
    shutdown();
}

// Producer: Adds a message to the queue (copy version)
void MessageQueue::push(const Message& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!shutdown_.load()) {
        queue_.push(message);
        cv_.notify_one();
    }
}

// Producer: Adds a message to the queue (move version)
void MessageQueue::push(Message&& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!shutdown_.load()) {
        queue_.push(std::move(message));
        cv_.notify_one();
    }
}

// Consumer: Blocks until a message is available and returns it
Message MessageQueue::pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !queue_.empty() || shutdown_.load(); });
    
    if (shutdown_.load() && queue_.empty()) {
        throw std::runtime_error("MessageQueue is shutdown and empty");
    }
    
    Message message = std::move(queue_.front());
    queue_.pop();
    return message;
}

// Consumer: Tries to pop a message with timeout
bool MessageQueue::tryPop(Message& message, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    bool result = cv_.wait_for(lock, timeout, [this] { 
        return !queue_.empty() || shutdown_.load(); 
    });
    
    if (!result || (shutdown_.load() && queue_.empty())) {
        return false;
    }
    
    message = std::move(queue_.front());
    queue_.pop();
    return true;
}

// Utility: Returns current queue size
size_t MessageQueue::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

// Utility: Checks if queue is empty
bool MessageQueue::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

// Management: Shuts down the queue
void MessageQueue::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_.store(true);
    cv_.notify_all();
}
