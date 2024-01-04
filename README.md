This repository contains solutions for Fly.io's [Gossip Glomers](https://fly.io/dist-sys/), a series of distributed systems challenges.

My solution for Challenge 4 is a bit different because it doesn't use the built-in KeyValue store service. Rather, it implements a state-based increment-only counter CRDT based on the paper [A comprehensive study of Convergent and Commutative Replicated Data Types](https://inria.hal.science/inria-00555588/document) by Shapiro et al.

Challenge 5b involves a linearizable key-value store. This [video lecture](https://www.youtube.com/watch?v=noUNH3jDLC0&ab_channel=MartinKleppmann) by Martin Klepmann is a great explanation of the idea of linearizability.