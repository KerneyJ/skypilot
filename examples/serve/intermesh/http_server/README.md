# Intermesh Integration Test for SkyServe

This example demonstrates and tests the Intermesh integration with SkyServe. It launches a simple HTTP service with multiple replicas to verify that Intermesh naming and endorsements work correctly.

## Prerequisites

1. **Install Intermesh on your local machine** (acts as root of trust):
   ```bash
   curl -sSL https://raw.githubusercontent.com/KerneyJ/intermesh-binaries/main/intermesh-linux-x86_64 \
     -o /usr/bin/intermesh
   chmod +x /usr/bin/intermesh
   ```

2. **Start Intermesh daemon**:
   ```bash
   # Note: Do NOT use --intercept flag. The laptop only acts as root of trust,
   # not as an L4 proxy. Laptop ↔ controller uses SSH port forwarding.
   # Only controller ↔ replica uses Intermesh mesh names.
   sudo intermesh daemon --log-file /var/log/intermesh.log &
   ```

3. **Verify daemon is running**:
   ```bash
   intermesh status
   ```

4. **Enable Intermesh in SkyPilot config** (`~/.sky/config.yaml`):
   ```yaml
   intermesh:
     enabled: true
   ```

5. **Configure firewall** (allow Intermesh gossip on TCP port 50053):
   
   **Linux (iptables)**:
   ```bash
   sudo iptables -I INPUT -p tcp --dport 50053 -j ACCEPT
   sudo iptables-save > /etc/iptables/rules.v4
   ```
   
   **Linux (firewalld)**:
   ```bash
   sudo firewall-cmd --permanent --add-port=50053/tcp
   sudo firewall-cmd --reload
   ```
   
   **Linux (ufw)**:
   ```bash
   sudo ufw allow 50053/tcp
   ```

## Running the Test

### Step 1: Launch the Service

```bash
cd examples/serve/intermesh/http_server
sky serve up http-test task.yaml
```

This will:
- Launch a controller cluster: `sky-serve-controller-http-test`
- Launch 2 replica clusters: `http-test-0` and `http-test-1`
- Each cluster gets Intermesh daemon installed and configured
- Nodes register with your local Intermesh daemon (root of trust)

### Step 2: Verify Intermesh Naming

Check that all nodes have registered with hierarchical names:

```bash
intermesh debug dump
```

**Expected output** should show:
```
controller.http-test.gcp.sky → <controller-ip>
head.replica-0.http-test.gcp.sky → <replica-0-head-ip>
head.replica-1.http-test.gcp.sky → <replica-1-head-ip>
```

**Key points to verify**:
- ✅ Controller has simplified naming: `controller.http-test.gcp.sky` (NOT `controller.sky-serve-controller-http-test.gcp.sky`)
- ✅ Replicas have restructured naming: `head.replica-0.http-test.gcp.sky` (NOT `head.http-test-0.gcp.sky`)
- ✅ All nodes have IP mappings

### Step 3: Check Service Status

```bash
sky serve status http-test --all
```

**Expected output**:
```
Service name: http-test
Status: READY
Replicas:
  - http-test-0: READY
  - http-test-1: READY
Load balancer: http://localhost:<port>
```

### Step 4: Test Load Balancer Connectivity

```bash
# Get load balancer port
LB_PORT=$(sky serve status http-test --no-format | grep -oP '(?<=:)\d+' | head -1)

# Test health endpoint
curl http://localhost:$LB_PORT/health

# Test info endpoint (shows which replica handled the request)
curl http://localhost:$LB_PORT/info
```

**Expected responses**:
- `/health`: `{"status": "healthy"}`
- `/info`: Shows hostname and cluster name

### Step 5: Verify Multiple Replicas Are Handling Requests

```bash
# Send 10 requests and see both replicas respond
for i in {1..10}; do
  curl -s http://localhost:$LB_PORT/info | jq -r '.cluster'
done
```

**Expected**: You should see both `http-test-0` and `http-test-1` in the output (load balancing is working).

### Step 6: Test Replica Replacement (Optional)

Verify that when a replica is terminated, Intermesh correctly re-registers the new replacement:

```bash
# Terminate one replica
sky down http-test-0 -y

# Wait for autoscaler to detect and launch replacement
# (typically 30-60 seconds)
watch -n 5 'sky serve status http-test --all'

# Once new replica is READY, verify Intermesh has updated registrations
intermesh debug dump | grep replica-0
```

**Expected**: New replica has:
- Different IMID (new VM, new identity)
- Different IP address
- Same mesh name: `head.replica-0.http-test.gcp.sky`

### Step 7: Inspect Node Configurations (Optional)

SSH into a node and check Intermesh status:

```bash
# SSH into controller
sky ssh sky-serve-controller-http-test

# Check Intermesh status
sudo intermesh status

# Check gossip peers
sudo intermesh debug dump
```

**Expected on controller**:
- Own IMID is registered
- Gossip peers include your laptop and replica nodes
- Can resolve replica names: `head.replica-0.http-test.gcp.sky`

## Cleanup

```bash
# Terminate the service (removes controller and all replicas)
sky serve down http-test -y

# Verify all clusters are gone
sky status
```

## Troubleshooting

### Issue: Nodes not registering with Intermesh

**Check**:
1. Is Intermesh daemon running on your laptop? (`intermesh status`)
2. Can nodes reach your laptop on port 50053? (`nc -zv <your-laptop-ip> 50053`)
3. Check SSH connectivity: `sky ssh sky-serve-controller-http-test`

**Debug**:
```bash
# On your laptop, check Intermesh logs
sudo tail -f /var/log/intermesh.log

# On a cluster node, check Intermesh status
sky ssh sky-serve-controller-http-test
sudo intermesh status
sudo tail -f /var/log/intermesh.log
```

### Issue: Wrong naming format

**Check**:
```bash
intermesh debug dump
```

If you see names like:
- `controller.sky-serve-controller-http-test.gcp.sky` (too verbose)
- `head.http-test-0.gcp.sky` (not restructured)

This indicates the custom naming logic isn't working. Check:
1. Is Intermesh enabled in config? (`cat ~/.sky/config.yaml`)
2. Check the implementation in `sky/backends/cloud_vm_ray_backend.py`

### Issue: Load balancer can't connect to replicas

**Note**: This is expected! Intermesh daemon does NOT yet support traffic interception. The load balancer still uses IP addresses to connect to replicas.

Future work: When Intermesh adds traffic interception, the load balancer will connect via mesh names like `http://head.replica-0.http-test.gcp.sky:8080`.

## What This Test Validates

✅ **Phase 1 Complete**: TODO comments added to mark future URL changes  
✅ **Phase 2 Complete**: Custom naming for controller and replicas  
- Controller: `controller.{service}.gcp.sky`  
- Replicas: `head.replica-{id}.{service}.gcp.sky`  

⏸️ **Phase 3 (Future)**: Load balancer URL changes blocked until Intermesh supports traffic interception

## Success Criteria

- [x] All nodes register with Intermesh  
- [x] Controller has simplified naming  
- [x] Replicas have restructured naming  
- [x] Load balancer can reach replicas (via IPs, not mesh names yet)  
- [x] Service scales up/down correctly  
- [x] Replica replacement creates new IMID + IP + re-registers  

## Next Steps

Once Intermesh daemon supports traffic interception:
1. Remove TODO comments in `sky/serve/replica_managers.py`
2. Implement mesh name URLs in `ReplicaInfo.url` property
3. Test mTLS encryption between load balancer and replicas
