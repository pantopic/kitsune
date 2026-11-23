# Kitsune

<img alt="Kitsune" title="Kitsune" src="https://raw.githubusercontent.com/pantopic/kitsune/main/_notes/kitsune-logo.png" align="right" width="360" />


A distributed queue that's mostly API compatible with [SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Welcome.html).

### Features

Basic functionality is WIP.

`SendMessageBatch` supports standard-queue message bodies and delays, including
per-entry failures. Message attributes, system attributes, and FIFO fields are
not yet supported and are rejected for batch entries.

Not included in basic functionality:
- Queue Tags
- Permissions
- Deduplication
- Dead Letter Queues

### Roadmap

This project is in alpha. Breaking changes should be expected until Beta.

- `v0.0.x` - Alpha
  - [ ] 80% API compatible with SQS
- `v0.1.x` - Beta
  - [ ] Add metrics
  - [ ] Test in production
- `v1.x.x` - General Availability
  - [ ] Proven long term stability in production
