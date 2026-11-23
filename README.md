# Kitsune

<img alt="Kitsune" title="Kitsune" src="https://raw.githubusercontent.com/pantopic/kitsune/main/_notes/kitsune-logo.png" align="right" width="360" />


A distributed queue that's mostly API compatible with [SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Welcome.html).

### Features

Basic functionality is WIP.

`SendMessage` and `SendMessageBatch` support standard message attributes and
the `AWSTraceHeader` system attribute. Other message system attributes and
FIFO fields are not yet supported. Batch sends report per-entry failures.
`DeleteMessageBatch` and `ChangeMessageVisibilityBatch` support per-entry
results for standard queues.
`ListQueues` supports pagination when `MaxResults` is specified.
Queue attributes include approximate visible, delayed, and in-flight counts.
Queue ARNs use region `us-east-1` and account `000000000000` by default, configurable as globals.
Message attribute selection supports exact names, `All`, `.*`, and `prefix.*`.
Receive system-attribute selection currently supports `AWSTraceHeader` and
`ApproximateReceiveCount`.
Long polling is not implemented: nonzero effective `WaitTimeSeconds` is
rejected rather than silently treated as a short poll.
Queue and message timestamp attributes require a real host wall clock and are
not yet supported.

New message deliveries use generation-bearing receipt handles. Existing active
legacy handles are supported under their previous validity rules, but older
module versions cannot use the new handles.

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
