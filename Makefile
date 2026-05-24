# RDMA Distributed Primitives — Makefile
# Single entry point for building, deploying, and running on CloudLab.

CONFIG    := cluster.toml
TIMESTAMP := $(shell date +%Y%m%d_%H%M%S)
SSH_KEY   := $(shell awk -F'"' '/^ssh_key/{ print $$2 }' $(CONFIG) 2>/dev/null)
SSH_OPTS  := -o StrictHostKeyChecking=no -o ConnectTimeout=10 $(if $(SSH_KEY),-i $(SSH_KEY))

# ── Benchmark params (pass-through) ─────────────────────────────────────────
# Usage: make run BENCH="PRIMITIVE=queue QUEUE_SIZE=1024 NUM_OPS=1000000"
# The Makefile doesn't interpret BENCH — it just forwards it as env vars.

RDMA_PORT ?= 6969
BENCH     ?=

# ── TOML helpers ─────────────────────────────────────────────────────────────

AWK_SERVERS = awk '/^\[servers\]/{f=1;next} /^\[/{f=0} f && /=/ && !/^\#/'
AWK_CLIENTS = awk '/^\[clients\]/{f=1;next} /^\[/{f=0} f && /=/ && !/^\#/'

# ── Parsed config ───────────────────────────────────────────────────────────

EXPERIMENT_USER := $(shell awk -F'"' '/^user/{ print $$2 }' $(CONFIG) 2>/dev/null)
REMOTE_DIR      := $(shell awk -F'"' '/^remote_dir/{ print $$2 }' $(CONFIG) 2>/dev/null)
IB_INTERFACE    := $(shell awk -F'"' '/^ib_interface/{ print $$2 }' $(CONFIG) 2>/dev/null)
IB_NETMASK      := $(shell awk -F'"' '/^ib_netmask/{ print $$2 }' $(CONFIG) 2>/dev/null)
IB_MTU          := $(shell awk -F'"' '/^ib_mtu/{ print $$2 }' $(CONFIG) 2>/dev/null)

SERVER_HOSTS := $(shell $(AWK_SERVERS) $(CONFIG) 2>/dev/null | awk -F'"' '{print $$2}' | awk '{print $$1}')
SERVER_IPS   := $(shell $(AWK_SERVERS) $(CONFIG) 2>/dev/null | awk -F'"' '{print $$2}' | awk '{print $$2}')
CLIENT_HOSTS := $(shell $(AWK_CLIENTS) $(CONFIG) 2>/dev/null | awk -F'"' '{print $$2}' | awk '{print $$1}')
CLIENT_IPS   := $(shell $(AWK_CLIENTS) $(CONFIG) 2>/dev/null | awk -F'"' '{print $$2}' | awk '{print $$2}')
SERVERS_CSV  := $(shell $(AWK_SERVERS) $(CONFIG) 2>/dev/null | awk -F'"' '{print $$2}' | awk '{print $$2}' | paste -sd, -)
ALL_IPS      := $(SERVER_IPS) $(CLIENT_IPS)
ALL_IPS_CSV  := $(shell echo "$(SERVER_IPS) $(CLIENT_IPS)" | tr ' ' ',')
ALL_HOSTS    := $(SERVER_HOSTS) $(CLIENT_HOSTS)

BUILD_CMD = clang++ -std=c++23 -O3 -march=native -ffast-math \
            src/*.cpp -Iinclude \
            -libverbs -lpthread -o rdma

.PHONY: all
all:
	clang++ -std=c++23 -O3 -Iinclude -c src/main.cpp
	clang++ -std=c++23 -O3 -Iinclude -c src/transport.cpp

.PHONY: test
test: deploy run

.PHONY: clean
clean:
	rm -f rdma *.o

# ── Config guard ─────────────────────────────────────────────────────────────

.PHONY: check-config
check-config:
	@test -f $(CONFIG) || { echo "ERROR: $(CONFIG) not found. Copy cluster.toml.example and fill it in."; exit 1; }
	@test -n "$(EXPERIMENT_USER)" || { echo "ERROR: could not parse user from $(CONFIG)"; exit 1; }

# ── Setup (runs once, re-runs if config or script changes) ──────────────────

.setup-done: $(CONFIG) scripts/setup.sh
	@echo "=== Setting up $(words $(ALL_HOSTS)) nodes ==="
	@hosts=( $(SERVER_HOSTS) $(CLIENT_HOSTS) ); \
	 ips=( $(SERVER_IPS) $(CLIENT_IPS) ); \
	 pids=(); \
	 for i in $$(seq 0 $$((  $${#hosts[@]} - 1 ))); do \
	     h=$${hosts[$$i]}; ip=$${ips[$$i]}; \
	     echo "  [$$i] $$h ($$ip)"; \
	     ( scp $(SSH_OPTS) scripts/setup.sh $(EXPERIMENT_USER)@$$h:/tmp/rdma-setup.sh && \
	       ssh $(SSH_OPTS) $(EXPERIMENT_USER)@$$h \
	           "sudo bash /tmp/rdma-setup.sh $(IB_INTERFACE) $$ip $(IB_NETMASK) $(IB_MTU) $(EXPERIMENT_USER)" \
	     ) & pids+=($$!); \
	 done; \
	 fail=0; for p in $${pids[@]}; do wait $$p || ((fail++)); done; \
	 [ $$fail -eq 0 ] || { echo "ERROR: $$fail node(s) failed"; exit 1; }
	@echo "  All-to-all IB ping..."
	@all_ips="$(SERVER_IPS) $(CLIENT_IPS)"; \
	 hosts=( $(SERVER_HOSTS) $(CLIENT_HOSTS) ); \
	 pids=(); \
	 for h in $${hosts[@]}; do \
	     ( for ip in $$all_ips; do \
	           until ssh $(SSH_OPTS) $(EXPERIMENT_USER)@$$h "ping -c 1 -W 2 $$ip" >/dev/null 2>&1; do \
	               sleep 1; \
	           done; \
	       done \
	     ) & pids+=($$!); \
	 done; \
	 for p in $${pids[@]}; do wait $$p; done
	@echo "  Ping complete"
	@touch .setup-done
	@echo "=== Setup complete ==="

# ── Deploy (rsync + build, runs setup first if needed) ──────────────────────

.PHONY: deploy
deploy: check-config .setup-done
	@echo "=== Deploying to $(words $(ALL_HOSTS)) nodes ==="
	@pids=(); \
	 for h in $(ALL_HOSTS); do \
	     echo "  $$h"; \
	     ( rsync -az --delete \
	         --exclude='.git' --exclude='.idea' --exclude='cmake-build-*' \
	         --exclude='cluster.toml' --exclude='results' --exclude='.DS_Store' \
	         --exclude='.setup-done' --exclude='*.pdf' \
	         -e "ssh $(SSH_OPTS)" \
	         ./ $(EXPERIMENT_USER)@$$h:$(REMOTE_DIR)/ && \
	       ssh $(SSH_OPTS) $(EXPERIMENT_USER)@$$h "cd $(REMOTE_DIR) && $(BUILD_CMD)" \
	     ) & pids+=($$!); \
	 done; \
	 fail=0; for p in $${pids[@]}; do wait $$p || ((fail++)); done; \
	 [ $$fail -eq 0 ] || { echo "ERROR: $$fail node(s) failed"; exit 1; }
	@echo "=== Deploy complete ==="

# ── Run (servers then clients) ──────────────────────────────────────────────

.PHONY: ping
ping: check-config
	@echo "=== All-to-all IB ping ==="
	@all_ips="$(SERVER_IPS) $(CLIENT_IPS)"; \
	 hosts=( $(SERVER_HOSTS) $(CLIENT_HOSTS) ); \
	 pids=(); \
	 for h in $${hosts[@]}; do \
	     ( for ip in $$all_ips; do \
	           until ssh $(SSH_OPTS) $(EXPERIMENT_USER)@$$h "ping -c 1 -W 2 $$ip" >/dev/null 2>&1; do \
	               sleep 1; \
	           done; \
	       done \
	     ) & pids+=($$!); \
	 done; \
	 for p in $${pids[@]}; do wait $$p; done
	@echo "=== Ping complete ==="

.PHONY: run
run: check-config
	@echo "=== Run $(TIMESTAMP) ==="
	@$(MAKE) --no-print-directory kill 2>/dev/null || true
	@$(MAKE) --no-print-directory ping
	@sleep 1
	@all_hosts=( $(ALL_HOSTS) ); \
	 pids=(); \
	 for i in $$(seq 0 $$(( $${#all_hosts[@]} - 1 ))); do \
	     echo "  node $$i: $${all_hosts[$$i]}"; \
	     ssh $(SSH_OPTS) $(EXPERIMENT_USER)@$${all_hosts[$$i]} \
	         "cd $(REMOTE_DIR) && sudo NODE_ID=$$i SERVERS=$(ALL_IPS_CSV) \
	          $(BENCH) ./rdma > /tmp/rdma-node-$$i.log 2>&1" & \
	     pids+=($$!); \
	 done; \
	 echo "  Waiting for all nodes..."; \
	 for p in $${pids[@]}; do wait $$p || true; done
	@mkdir -p results/$(TIMESTAMP)
	@all_hosts=( $(ALL_HOSTS) ); \
	 for i in $$(seq 0 $$(( $${#all_hosts[@]} - 1 ))); do \
	     scp $(SSH_OPTS) $(EXPERIMENT_USER)@$${all_hosts[$$i]}:/tmp/rdma-node-$$i.log results/$(TIMESTAMP)/ 2>/dev/null || true; \
	 done
	@echo "=== Done. Results in results/$(TIMESTAMP)/ ==="

# ── Kill ────────────────────────────────────────────────────────────────────

.PHONY: kill
kill: check-config
	@for h in $(ALL_HOSTS); do \
	     ssh $(SSH_OPTS) $(EXPERIMENT_USER)@$$h "sudo pkill -9 rdma" 2>/dev/null || true; \
	 done
	@echo "Killed"

# ── Logs ────────────────────────────────────────────────────────────────────

.PHONY: logs
logs: check-config
ifndef NODE
	@echo "Usage: make logs NODE=node0"
else
	@host=$$(awk '/^$(NODE) *=/' $(CONFIG) | awk -F'"' '{print $$2}' | awk '{print $$1}'); \
	 [ -n "$$host" ] || { echo "Node '$(NODE)' not found"; exit 1; }; \
	 ssh $(SSH_OPTS) $(EXPERIMENT_USER)@$$host "tail -f /tmp/rdma-*.log"
endif
