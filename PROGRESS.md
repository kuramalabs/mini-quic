# Progress

## Phase 1: UDP fundamentals

- [x] UDP echo server (receive from anyone, send back to that addr)
- [x] Track connected/active senders
- [x] Handle out-of-order / duplicate / late arrival packets
- [x] Sequence numbers
- [x] ACKs
- [x] Retransmission timer

## Phase 2: Reliability & Congestion Control

- [x] RTT estimation (round-trip time)
- [x] Adaptive retransmit timeout (replace hardcoded 5s with estimated RTT × multiplier)
- [x] Max retry limit
- [ ] Congestion window (limit how many unacked packets can be in-flight at once)
- [ ] Slow start (grow window on successful ACKs)
- [ ] Congestion avoidance (back off window on loss/timeout detection)