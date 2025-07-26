# Runbook: Fraud Detection System Down

## Alert Details
- **Alert Name**: FraudDetectionSystemDown
- **Severity**: Critical
- **Service**: Fraud Detection Engine
- **Business Impact**: Critical - Fraud protection disabled, security risk

## Immediate Response (0-5 minutes)

### Step 1: Emergency Measures
```bash
# CRITICAL: Enable conservative fraud rules immediately
kubectl patch configmap fraud-rules -p '{"data":{"mode":"conservative"}}'

# Check if backup fraud detection is available
kubectl get pods -l app=fraud-detection-backup
kubectl scale deployment/fraud-detection-backup --replicas=3
```

### Step 2: Assess Primary System
```bash
# Check service status
kubectl get pods -l app=fraud-detection
kubectl logs deployment/fraud-detection --tail=50

# Check service health
curl -f http://fraud-detection.internal/health
```

### Step 3: Immediate Communication
- **PRIORITY**: Notify security team immediately
- Update #security-alerts channel
- Page fraud detection team lead
- Notify risk management team

## Business Impact Assessment

### Revenue Risk
- All transactions potentially vulnerable
- False positives may increase with conservative rules
- Customer trust impact if fraud increases

### Security Risk
- Fraudulent transactions may go undetected
- Compliance violations possible
- Potential financial losses

### Immediate Containment
```bash
# Enable additional verification for high-value transactions
kubectl patch configmap payment-rules -p '{"data":{"high_value_threshold":"100","require_2fa":"true"}}'

# Reduce transaction limits temporarily
kubectl patch configmap transaction-limits -p '{"data":{"daily_limit":"1000","single_limit":"500"}}'
```

## Investigation (5-15 minutes)

### Check Fraud Detection Services
```bash
# Main fraud detection service
kubectl describe deployment fraud-detection
kubectl get events --field-selector involvedObject.name=fraud-detection

# ML model serving
kubectl get pods -l app=fraud-ml-model
kubectl logs deployment/fraud-ml-model

# Rule engine
kubectl get pods -l app=fraud-rules-engine
kubectl logs deployment/fraud-rules-engine
```

### Check Dependencies
```bash
# Machine learning pipeline
kubectl get pods -l app=ml-pipeline
spark-submit --class HealthCheck fraud-ml-health-check.jar

# Feature store
kubectl exec -it deployment/feature-store -- redis-cli ping
kubectl exec -it deployment/feature-store -- redis-cli info memory

# Database connectivity
psql -h fraud-db -U fraud_user -c "SELECT count(*) FROM fraud_models WHERE active = true"
```

### Check Data Pipeline
```bash
# Kafka topics for fraud data
kubectl exec kafka-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --list | grep fraud
kubectl exec kafka-0 -- kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fraud-detection

# Data freshness
psql -h fraud-db -U fraud_user -c "SELECT max(created_at) FROM transaction_features"
```

## Common Root Causes and Solutions

### 1. ML Model Failure

**Symptoms:**
- Model loading errors
- Prediction timeouts
- Model accuracy alerts

**Solution:**
```bash
# Check model status
kubectl exec -it deployment/fraud-ml-model -- curl localhost:8080/models

# Rollback to previous model version
kubectl set image deployment/fraud-ml-model fraud-ml=fraud/ml-model:v1.2.3

# Verify model health
kubectl exec -it deployment/fraud-ml-model -- python -c "
import joblib
model = joblib.load('/models/fraud_model.pkl')
print('Model loaded successfully')
print(f'Model version: {model.version}')
"
```

### 2. Database Performance Issues

**Symptoms:**
- Slow fraud rule queries
- Connection pool exhaustion
- Lock timeouts

**Solution:**
```bash
# Check database performance
psql -h fraud-db -U fraud_user -c "SELECT * FROM pg_stat_activity WHERE state = 'active'"

# Check for long-running queries
psql -h fraud-db -U fraud_user -c "
SELECT pid, now() - pg_stat_activity.query_start AS duration, query
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes'
"

# Restart fraud detection to reset connections
kubectl rollout restart deployment/fraud-detection
```

### 3. Feature Store Issues

**Symptoms:**
- Redis connection errors
- Feature lookup timeouts
- Stale feature data

**Solution:**
```bash
# Check Redis status
kubectl exec -it deployment/feature-store -- redis-cli info replication
kubectl exec -it deployment/feature-store -- redis-cli client list

# Test feature retrieval
kubectl exec -it deployment/feature-store -- redis-cli get "user:12345:features"

# Clear cache if needed
kubectl exec -it deployment/feature-store -- redis-cli flushall
```

### 4. Kafka Consumer Lag

**Symptoms:**
- High consumer lag
- Transaction processing delays
- Missing fraud events

**Solution:**
```bash
# Check consumer lag
kubectl exec kafka-0 -- kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fraud-detection

# Reset consumer group if needed
kubectl exec kafka-0 -- kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group fraud-detection --topic transactions --to-latest --execute

# Scale up fraud detection consumers
kubectl scale deployment/fraud-detection --replicas=5
```

## Recovery Procedures

### Immediate Recovery
```bash
# 1. Start backup fraud detection
kubectl scale deployment/fraud-detection-backup --replicas=3
kubectl patch service fraud-detection -p '{"spec":{"selector":{"app":"fraud-detection-backup"}}}'

# 2. Deploy known good version
kubectl set image deployment/fraud-detection fraud-detection=fraud/detection:$LAST_KNOWN_GOOD_VERSION

# 3. Verify fraud detection is working
curl -X POST http://fraud-detection.internal/check \
  -H "Content-Type: application/json" \
  -d '{"user_id":"test","amount":100,"merchant":"test"}'
```

### Full System Recovery
```bash
# 1. Restart all fraud detection components
kubectl rollout restart deployment/fraud-detection
kubectl rollout restart deployment/fraud-ml-model
kubectl rollout restart deployment/fraud-rules-engine

# 2. Verify ML pipeline
spark-submit --class FraudDetectionHealthCheck fraud-health.jar

# 3. Test end-to-end flow
python test_fraud_detection.py --endpoint http://fraud-detection.internal
```

## Escalation Criteria

### Escalate to Security Team Lead if:
- System down for >10 minutes
- ML model completely unavailable
- Feature store corrupted
- Suspected security incident

### Escalate to CISO if:
- System down for >30 minutes
- Evidence of actual fraud getting through
- Compliance implications
- Customer data at risk

### Escalate to CTO if:
- System down for >1 hour
- Multiple recovery attempts failed
- Business continuity at risk

## Communication Templates

### Security Alert
```
🚨 SECURITY ALERT: Fraud detection system is down.
Conservative fraud rules have been enabled.
Transaction limits temporarily reduced.
Security team investigating.
```

### Risk Management Update
```
Fraud detection system outage - Duration: [X] minutes
Mitigation: Conservative rules active, reduced limits
Estimated fraud exposure: $[AMOUNT]
Recovery ETA: [TIME]
```

### Business Stakeholder Update
```
Our fraud detection system is experiencing issues.
Enhanced security measures are in place.
Transaction processing may be slower.
Investigating and working on resolution.
```

## Business Continuity Measures

### During Outage
1. **Enable Conservative Rules**: Higher friction, lower fraud risk
2. **Reduce Limits**: Minimize potential fraud impact
3. **Manual Review**: Flag high-value transactions for manual review
4. **Enhanced Monitoring**: Increase human oversight

### Risk Mitigation
```bash
# Enable manual review queue
kubectl patch configmap payment-config -p '{"data":{"manual_review_threshold":"200"}}'

# Increase verification requirements
kubectl patch configmap auth-config -p '{"data":{"require_2fa_amount":"50"}}'

# Alert on unusual patterns
kubectl patch configmap monitoring-config -p '{"data":{"alert_on_velocity":"true"}}'
```

## Post-Incident Actions

### Immediate (within 2 hours)
- [ ] Verify fraud detection fully operational
- [ ] Review transactions processed during outage
- [ ] Disable conservative measures if appropriate
- [ ] Document incident timeline

### Short-term (within 24 hours)
- [ ] Analyze any fraud that occurred during outage
- [ ] Review and improve backup procedures
- [ ] Update monitoring and alerting
- [ ] Conduct incident review

### Long-term (within 1 week)
- [ ] Implement additional redundancy
- [ ] Improve automated failover
- [ ] Update business continuity procedures
- [ ] Share lessons learned with organization

## Monitoring and Metrics

### Key Metrics During Recovery
- Fraud detection response time (< 100ms)
- Model prediction accuracy (> 95%)
- False positive rate (< 5%)
- System availability (> 99.9%)

### Business Metrics to Monitor
- Transaction approval rate
- Revenue impact
- Customer complaints
- Chargeback rates

### Dashboards
- [Fraud Detection Overview](http://grafana:3000/d/fraud-overview)
- [ML Model Performance](http://grafana:3000/d/ml-performance)
- [Security Metrics](http://grafana:3000/d/security-metrics)

## Prevention Strategies

### Technical Improvements
- Add health checks to all components
- Implement circuit breakers
- Add model redundancy
- Improve error handling

### Operational Improvements
- Automate more recovery procedures
- Improve monitoring coverage
- Add chaos engineering tests
- Regular disaster recovery drills

### Business Continuity
- Enhance backup fraud detection
- Improve manual review processes
- Add real-time fraud monitoring
- Develop fraud response playbooks

---

**Runbook Version**: 1.0
**Last Updated**: 2025-01-26
**Next Review**: 2025-02-26
**Owner**: Security Team / SRE Team
**Emergency Contact**: Security Team Lead (+1-555-SECURITY)
