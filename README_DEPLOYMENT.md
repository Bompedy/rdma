# Quick Start: Running RDMA Watch Benchmarks

## Setup (One Time)

### 1. Edit `nodes.txt` with your server hostnames:
```bash
# nodes.txt
stevie98@apt004.apt.emulab.net
stevie98@apt008.apt.emulab.net
stevie98@apt013.apt.emulab.net
# Add more nodes as needed...
```

### 2. Deploy to all nodes:
```bash
cd /Users/yingjianwu/Desktop/rdma
./deploy.sh
```

This will:
- ✅ Auto-detect IPs from all nodes
- ✅ Update configuration files
- ✅ Copy code to all nodes
- ✅ Install dependencies
- ✅ Compile on all nodes

## Running Benchmarks

### Run Synra Watch (One-Sided RDMA):
```bash
./run_synra_watch.sh
```

### Run Mu Watch (Leader-Based):
```bash
./run_mu_watch.sh
```

## Results

Results are saved to:
- `synra_watch_results.txt`
- `mu_watch_results.txt`

Look for the **CSV line** at the end:
```
CSV: watch,0,4,1000,8,0.50,80000,X.XXX,XXXXX,XX.XX,...
     ^       ^  ^    ^     ^               ^
     |       |  |    |     |               +-- Mean latency (μs)
     |       |  |    |     +-- Goodput (ops/sec)
     |       |  |    +-- Active window
     |       |  +-- Locks
     |       +-- Clients
     +-- Strategy
```

## Compare Results

Synra should show:
- **~20-100x higher goodput** (ops/sec)
- **~20-100x lower latency** (μs)

## Adding More Nodes

Just add more lines to `nodes.txt` and re-run `./deploy.sh`!

## Troubleshooting

**"Connection refused":** Check firewall or node connectivity

**"Command not found":** Make sure scripts are executable:
```bash
chmod +x deploy.sh run_synra_watch.sh run_mu_watch.sh
```

**"No nodes found":** Edit `nodes.txt` and remove comment lines (#)
