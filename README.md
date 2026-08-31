# ⚡ Redis Social Data Experiment

A Python project exploring Redis as a NoSQL database through a simplified social-network data model.

The application models users and posts, stores relationships between them, and experiments with Redis atomic operations and concurrent updates.

## 🎯 Project Goals

The project was created to practice:

* Redis data structures
* NoSQL data modeling
* Redis OM
* Key-value storage
* Hash operations
* Atomic counters
* Concurrent operations
* Python integration with Redis

## 🛠️ Technologies

* Python
* Redis
* Redis Cloud
* Redis OM
* Python threading

## ✨ Features

### User Management

Users contain information such as:

* Username
* First name
* Last name
* Email
* Associated posts

User information is stored in Redis hashes.

### Posts

Users can create posts containing text content.

Posts maintain a reference to their author and their current number of likes.

### Likes

Likes are updated using Redis atomic increment operations:

```text
HINCRBY
```

This makes Redis suitable for counters that may receive many concurrent updates.

### Concurrency Experiment

The project includes an experiment using multiple Python threads to simulate many users incrementing the like counter of the same post concurrently.

The goal is to demonstrate the atomic behavior and performance characteristics of Redis counters.

## 🧱 Simplified Data Model

```text
User
│
├── username
├── first_name
├── last_name
├── email
└── posts[]
        │
        ▼
       Post
        ├── user_id
        ├── content
        └── likes
```

## 📁 Project Structure

```text
redis/
│
└── base.py
```

## 🚀 Setup

### Requirements

* Python 3
* Redis instance or Redis Cloud database

Install dependencies:

```bash
pip install redis redis-om
```

## 🔐 Environment Variables

Redis credentials should never be committed to the repository.

Configure them as environment variables instead:

```env
REDIS_HOST=your-redis-host
REDIS_PORT=your-redis-port
REDIS_PASSWORD=your-redis-password
```

Then load those values from the application configuration.

Example:

```python
import os

client = get_redis_connection(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)
```

Create a `.gitignore` containing:

```text
.env
.venv/
__pycache__/
```

## 🧪 Concurrency Test

The project simulates multiple threads updating the same Redis counter.

Redis' atomic increment operation prevents the traditional read-modify-write race condition for the counter itself.

This experiment demonstrates why Redis is commonly used for use cases such as:

* Likes
* Views
* Votes
* Metrics
* Rate counters
* Real-time statistics

## 🎯 What I Learned

Through this project I practiced:

* Designing simple NoSQL data models
* Working with Redis hashes
* Managing relationships between Redis entities
* Serializing data with JSON
* Using atomic Redis operations
* Working with Redis Cloud
* Simulating concurrent requests with Python threads
* Understanding how Redis handles shared counters

## ⚠️ Security

Database credentials must be configured through environment variables.

Never commit passwords, tokens or connection secrets to the repository.

## 👨‍💻 Author

**Francisco Parra**

Software Development student interested in backend development, databases, QA and software engineering.
