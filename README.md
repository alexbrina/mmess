# mmess
Simple RabbitMQ Mass Messaging Utility

## Basic Usage

### Publish

```bash
# For a list of options.
mmess publish -h

# Publish one message sized 1kb per second to exchange named my-topic.
mmess publish -e my-topic

# Publish one message sized 2kb each 100 milliseconds to exchange named my-topic
mmess publish -e my-topic -i 100 -s 2048
```

### Subscribe

```bash
# For a list of options.
mmess subscribe -h

# Subscribe to queue named my-topic with handling sleep time of 1 second.
mmess subscribe -q my-topic

# Subscribe to 2 queues named my-topic and my-other-topic with handling sleep time of 100 milliseconds.
mmess subscribe -i 100 -q my-topic my-other-topic
```