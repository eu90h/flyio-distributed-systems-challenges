This repository contains solutions for Fly.io's [Gossip Glomers](https://fly.io/dist-sys/), a series of distributed systems challenges.

My solution for Challenge 4 is a bit different because it doesn't use the built-in KeyValue store service. Rather, it implements a state-based increment-only counter CRDT based on the paper [A comprehensive study of Convergent and Commutative Replicated Data Types](https://inria.hal.science/inria-00555588/document) by Shapiro et al.
