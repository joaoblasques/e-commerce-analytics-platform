# Runbook: API Service Down

## Alert Details
- **Alert Name**: APIServiceDown
- **Severity**: Critical
- **Service**: API Gateway
- **Business Impact**: High - Customer-facing services unavailable

## Immediate Response (0-5 minutes)

### Step 1: Assess Scope
```bash
# Check service status
kubectl get pods -l app=ecap-api
curl -I http://api.ecap.local/health

# Check load balancer status
aws elbv2 describe-target-health --target-group-arn $API_TARGET_GROUP_ARN
```

### Step 2: Quick Remediation Attempts
```bash
# Restart unhealthy pods (automated, but can be manual)
kubectl rollout restart deployment/ecap-api

# Check for recent deployments
kubectl rollout history deployment/ecap-api

# Scale up if needed
kubectl scale deployment/ecap-api --replicas=5
```

### Step 3: Immediate Communication
- Update status page: "Investigating API service issues"
- Notify #incidents channel in Slack
- If customer reports increasing, escalate immediately

## Investigation (5-15 minutes)

### Check Infrastructure
```bash
# Node health
kubectl get nodes
kubectl describe node $NODE_NAME

# Cluster resources
kubectl top nodes
kubectl top pods -n default

# Ingress controller
kubectl get ingress
kubectl logs -n ingress-nginx deployment/nginx-ingress-controller
```

### Check Application Health
```bash
# Application logs
kubectl logs deployment/ecap-api --tail=100
kubectl logs deployment/ecap-api --previous

# Database connectivity
kubectl exec -it deployment/ecap-api -- psql -h $DB_HOST -U $DB_USER -c "SELECT 1"

# Redis connectivity
kubectl exec -it deployment/ecap-api -- redis-cli -h $REDIS_HOST ping
```

### Check Dependencies
```bash
# Database status
kubectl get pods -l app=postgres
psql -h $DB_HOST -U $DB_USER -c "SELECT version()"

# Message broker
kubectl get pods -l app=kafka
kubectl exec kafka-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --list

# Cache status
kubectl get pods -l app=redis
kubectl exec redis-0 -- redis-cli info replication
```

## Common Root Causes and Solutions

### 1. Resource Exhaustion

**Symptoms:**
- Pods in Pending state
- High memory/CPU usage
- OOMKilled events

**Solution:**
```bash
# Check resource usage
kubectl top pods
kubectl describe pod $POD_NAME

# Scale up resources
kubectl patch deployment ecap-api -p '{"spec":{"template":{"spec":{"containers":[{"name":"ecap-api","resources":{"requests":{"memory":"1Gi","cpu":"500m"},"limits":{"memory":"2Gi","cpu":"1000m"}}}]}}}}'

# Scale up replicas
kubectl scale deployment/ecap-api --replicas=10
```

### 2. Database Connection Issues

**Symptoms:**
- Connection timeout errors
- "too many connections" errors
- Slow query performance

**Solution:**
```bash
# Check database connections
psql -h $DB_HOST -U $DB_USER -c "SELECT count(*) FROM pg_stat_activity"

# Check for blocking queries
psql -h $DB_HOST -U $DB_USER -c "SELECT * FROM pg_stat_activity WHERE state = 'active' AND query_start < NOW() - INTERVAL '5 minutes'"

# Restart API to reset connections
kubectl rollout restart deployment/ecap-api
```

### 3. Configuration Issues

**Symptoms:**
- Service starts but immediately crashes
- Configuration validation errors
- Missing environment variables

**Solution:**
```bash
# Check configuration
kubectl get configmap ecap-api-config -o yaml
kubectl get secret ecap-api-secrets -o yaml

# Validate configuration
kubectl exec -it deployment/ecap-api -- cat /app/config/app.yml

# Check environment variables
kubectl exec -it deployment/ecap-api -- env | grep ECAP_
```

### 4. Network Issues

**Symptoms:**
- DNS resolution failures
- Load balancer health checks failing
- Service mesh issues

**Solution:**
```bash
# Test DNS resolution
kubectl exec -it deployment/ecap-api -- nslookup postgres.default.svc.cluster.local

# Check service endpoints
kubectl get endpoints ecap-api

# Test load balancer
curl -I http://$LOAD_BALANCER_URL/health

# Check network policies
kubectl get networkpolicy
```

## Recovery Procedures

### Full Service Recovery
```bash
# 1. Stop traffic
kubectl patch service ecap-api -p '{"spec":{"selector":{"app":"maintenance"}}}'

# 2. Deploy healthy version
kubectl set image deployment/ecap-api ecap-api=ecap/api:$LAST_KNOWN_GOOD_VERSION

# 3. Wait for rollout
kubectl rollout status deployment/ecap-api

# 4. Restore traffic
kubectl patch service ecap-api -p '{"spec":{"selector":{"app":"ecap-api"}}}'

# 5. Verify health
curl http://api.ecap.local/health
```

### Database Recovery
```bash
# If database is the issue
# 1. Check database status
kubectl get pods -l app=postgres
kubectl logs postgres-0

# 2. Restart database if needed
kubectl delete pod postgres-0

# 3. Verify data integrity
psql -h $DB_HOST -U $DB_USER -c "SELECT count(*) FROM users"

# 4. Restart API services
kubectl rollout restart deployment/ecap-api
```

## Escalation Criteria

### Escalate to Level 2 (Engineering Manager) if:
- Service down for >15 minutes
- Multiple failed restart attempts
- Database or infrastructure issues detected
- Customer escalations received

### Escalate to Level 3 (CTO) if:
- Service down for >30 minutes
- Data integrity concerns
- Security incident suspected
- Major customer impact

## Communication Templates

### Initial Status Update
```
We are investigating reports of API service connectivity issues.
Our team is actively working on a resolution.
Updates will be provided every 15 minutes.
```

### Service Restored
```
The API service issue has been resolved.
All services are operating normally.
We will continue to monitor the situation closely.
```

### Extended Outage
```
We continue to work on resolving the API service issue.
Current ETA for resolution: [TIME]
We sincerely apologize for the inconvenience.
```

## Post-Incident Actions

### Immediate (within 24 hours)
- [ ] Verify all systems are stable
- [ ] Document timeline of events
- [ ] Identify root cause
- [ ] Update monitoring if needed

### Follow-up (within 1 week)
- [ ] Conduct post-incident review
- [ ] Update runbooks based on lessons learned
- [ ] Implement preventive measures
- [ ] Communicate findings to stakeholders

## Monitoring and Metrics

### Key Metrics to Monitor
- API response time (p95 < 1s)
- Error rate (< 0.1%)
- Throughput (requests/minute)
- Pod restart rate

### Dashboards
- [API Overview Dashboard](http://grafana:3000/d/api-overview)
- [Infrastructure Health](http://grafana:3000/d/infrastructure)
- [Business Metrics](http://grafana:3000/d/business-metrics)

### Alerts
- APIServiceDown (this alert)
- HighAPIResponseTime
- HighErrorRate
- APICapacityAlert

## Prevention Strategies

### Short-term
- Increase resource limits
- Add more health checks
- Improve error handling
- Add circuit breakers

### Long-term
- Implement chaos engineering
- Add redundancy across regions
- Improve observability
- Automate more recovery procedures

---

**Runbook Version**: 1.0
**Last Updated**: 2025-01-26
**Next Review**: 2025-04-26
**Owner**: SRE Team
