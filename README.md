# RDMA Distributed Primitives

One-sided RDMA implementations of distributed primitives (locks, queues, latches, etc.) over InfiniBand, built for benchmarking on [CloudLab](https://www.cloudlab.us/).

## Project Structure

```
Makefile                 orchestrates everything: build, deploy, run
cluster.toml.example     reference config — copy and fill in per experiment
scripts/setup.sh         idempotent node provisioning (runs on remote nodes)
src/                     C++ source
include/rdma/            headers
```

## Requirements

- A CloudLab experiment with InfiniBand connectivity between nodes
- SSH access to all nodes from your local machine
- Clang with C++23 support (installed automatically by `make setup`)

## Quick Start

**1. Configure your experiment**

```sh
cp cluster.toml.example cluster.toml
```

Edit `cluster.toml` with your CloudLab node hostnames and InfiniBand IPs.

**2. Deploy**

```sh
make deploy
```

On first run, this provisions all nodes (RDMA packages, Clang, InfiniBand config, hugepages, CPU governors), then rsyncs source and builds remotely. Subsequent deploys skip setup unless `cluster.toml` or `scripts/setup.sh` changed.

**3. Run**

```sh
make run
make run BENCH="PRIMITIVE=queue QUEUE_SIZE=1024 NUM_OPS=1000000"
```

Starts servers sequentially, then clients in parallel. Results are collected into `results/<timestamp>/`.

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make deploy` | Provision nodes (first time), rsync source, build remotely |
| `make run` | Start servers, start clients, collect results |
| `make kill` | Kill rdma processes on all nodes |
| `make logs NODE=node0` | Tail a node's output live |

## Cluster Config

`cluster.toml` describes your CloudLab experiment. It's gitignored since it changes every time you get new machines.

```toml
[experiment]
user = "your-username"
ssh_key = "~/.ssh/cloudlab"
remote_dir = "/local/rdma"
ib_interface = "ibp8s0"
ib_netmask = "255.255.255.0"
ib_mtu = 65520

[servers]
node0 = "node0.utah.cloudlab.us  192.168.1.1"
node1 = "node1.utah.cloudlab.us  192.168.1.2"
node2 = "node2.utah.cloudlab.us  192.168.1.3"

[clients]
client0 = "node3.utah.cloudlab.us  192.168.1.4"
```

- `ssh_key` — path to the private key used for all SSH/SCP/rsync to nodes
- Node ID and machine ID are determined by order in the config file

## Benchmarks

*Coming soon.*
