# SuperClaude ECAP Configuration Guide

## Cost-Efficient Command Shortcuts

### Data Engineering Shortcuts
```bash
# Kafka Development
alias sc-kafka="/implement kafka_component --framework kafka --persona-backend --uc"
alias sc-kafka-analyze="/analyze src/data_ingestion/ --focus kafka --persona-backend --uc"
alias sc-kafka-improve="/improve src/data_ingestion/ --focus performance --scope module --uc"

# Spark Development
alias sc-spark="/implement spark_job --framework spark --persona-backend --c7 --uc"
alias sc-spark-analyze="/analyze src/analytics/ --focus spark --persona-backend --uc"
alias sc-spark-optimize="/improve src/analytics/ --focus performance --persona-performance --uc"

# Data Lake Operations
alias sc-datalake="/implement datalake_feature --framework delta --persona-backend --c7 --uc"
alias sc-datalake-analyze="/analyze src/data_lake/ --focus architecture --persona-architect --uc"
```

### API Development Shortcuts
```bash
# FastAPI Development
alias sc-api="/implement api_endpoint --framework fastapi --persona-backend --uc"
alias sc-api-analyze="/analyze src/api/ --focus architecture --persona-backend --uc"
alias sc-api-secure="/analyze src/api/ --focus security --persona-security --uc"
alias sc-api-optimize="/improve src/api/ --focus performance --persona-performance --uc"

# Authentication & Authorization
alias sc-auth="/implement auth_feature --framework fastapi --persona-security --c7 --uc"
alias sc-auth-analyze="/analyze src/api/auth/ --focus security --persona-security --uc"
```

### Dashboard Development Shortcuts
```bash
# Streamlit Development
alias sc-dashboard="/implement dashboard_component --framework streamlit --persona-frontend --magic --uc"
alias sc-dash-analyze="/analyze src/dashboard/ --focus ux --persona-frontend --uc"
alias sc-dash-improve="/improve src/dashboard/ --focus accessibility --persona-frontend --uc"
```

### Quality Assurance Shortcuts
```bash
# Analysis Commands
alias sc-analyze-full="/analyze --scope project --delegate auto --uc"
alias sc-analyze-quick="/analyze --scope module --focus quality --uc"
alias sc-health="/analyze dev-environment --focus health --persona-devops --uc"

# Quality Improvement
alias sc-improve-quality="/improve --focus quality --validate --persona-refactorer --uc"
alias sc-improve-security="/improve --focus security --persona-security --uc"
alias sc-improve-performance="/improve --focus performance --persona-performance --uc"

# Testing & Troubleshooting
alias sc-test="/troubleshoot test-failures --persona-qa --play --uc"
alias sc-debug="/troubleshoot --think --seq --uc"
```

### Documentation & Git Shortcuts
```bash
# Documentation Generation
alias sc-doc="/document --persona-scribe --c7 --uc"
alias sc-doc-api="/document src/api/ --focus api-docs --persona-scribe --uc"
alias sc-doc-setup="/document setup-guide --persona-scribe --uc"

# Git Operations
alias sc-commit="/git commit --persona-scribe --uc"
alias sc-pr="/git pr create --persona-scribe --c7 --uc"
alias sc-release="/git release --persona-scribe --c7 --uc"
```

## Session Management for Cost Control

### Session Startup Template
```bash
#!/bin/bash
# SuperClaude ECAP Development Session
echo "🚀 Starting SuperClaude ECAP Session - Cost Optimized"

# Establish session goals and token budget
/analyze session-goals --uc --focus planning

# Load essential project context
/load @ai_docs/CLAUDE.md --uc
/load @ECAP_tasklist.md --uc --focus current-phase

# Set development focus for the session
echo "Current focus areas:"
echo "1. Data Engineering (Kafka/Spark)"
echo "2. API Development (FastAPI)"
echo "3. Dashboard (Streamlit)"
echo "4. Analytics (ML/AI)"
echo "5. Infrastructure (Docker/Terraform)"

# Initialize TODO tracking
/todowrite session-tasks --uc
```

### Session Monitoring Template
```bash
#!/bin/bash
# Monitor token usage throughout session
function sc-monitor() {
    echo "📊 Session Progress Check"
    echo "Current token usage: [Track manually]"
    echo "Efficiency target: 30-50% reduction"
    echo "Tasks completed: $(grep -c 'completed' todolist.md)"
    echo "Quality gates passed: [Track validation results]"
}
```

### Session Cleanup Template
```bash
#!/bin/bash
# End session with summary and lessons learned
echo "📝 Ending SuperClaude ECAP Session"

# Generate session summary
/document session-summary --persona-scribe --uc --focus:
# - Tasks completed
# - Token efficiency achieved
# - Quality metrics
# - Lessons learned
# - Next session preparation

# Update project knowledge base
/document lessons-learned --persona-scribe --uc
```

## Token Budget Management

### Daily Development Budget
- **Analysis Phase**: 25% of daily budget
- **Implementation Phase**: 50% of daily budget
- **Quality Assurance**: 15% of daily budget
- **Documentation**: 10% of daily budget

### Emergency Protocols
When approaching 90% of daily budget:
1. Switch to `--answer-only --uc` mode
2. Batch remaining questions for next session
3. Focus on critical bug fixes only
4. Use existing documentation and examples

### Cost Tracking Template
```markdown
## Daily SuperClaude Cost Tracking

### Session: YYYY-MM-DD
- **Start Time**: HH:MM
- **End Time**: HH:MM
- **Token Budget**: XXXX tokens
- **Tokens Used**: XXXX tokens
- **Efficiency**: XX% reduction achieved
- **Tasks Completed**: X tasks
- **Quality Gates**: X/X passed

### Commands Used (with efficiency)
- `/analyze`: X calls, XX% reduction
- `/implement`: X calls, XX% reduction
- `/improve`: X calls, XX% reduction
- `/document`: X calls, XX% reduction

### Lessons Learned
- [What worked well for efficiency]
- [What could be optimized]
- [Patterns to replicate]

### Next Session Preparation
- [Priority tasks]
- [Context to load]
- [Efficiency targets]
```

## Framework-Specific Patterns

### Kafka Development Pattern
```bash
# Efficient Kafka development workflow
sc-kafka-analyze  # Understand current state
sc-kafka          # Implement feature
sc-improve-quality # Validate implementation
sc-commit         # Professional commit message
sc-doc           # Generate documentation
```

### Spark Analytics Pattern
```bash
# Efficient Spark job development
sc-spark-analyze  # Analyze requirements
sc-spark         # Implement job with Context7 patterns
sc-spark-optimize # Performance optimization
sc-test          # Validate with sample data
sc-commit        # Document and commit
```

### API Development Pattern
```bash
# Efficient FastAPI development
sc-api-analyze   # Understand API requirements
sc-api           # Implement endpoint
sc-api-secure    # Security validation
sc-api-optimize  # Performance tuning
sc-commit        # Professional documentation
```

### Dashboard Development Pattern
```bash
# Efficient Streamlit development
sc-dash-analyze  # UX requirements analysis
sc-dashboard     # Implement with Magic MCP
sc-dash-improve  # Accessibility and polish
sc-test          # E2E testing with Playwright
sc-commit        # Document component
```

## Quality Gates Integration

### SuperClaude 8-Step Validation Commands
```bash
# Run all quality gates efficiently
alias sc-validate-all="/improve implementation --focus quality --validate --uc"

# Individual quality gates (when needed)
alias sc-validate-syntax="poetry run black src/ && poetry run flake8 src/"
alias sc-validate-types="poetry run mypy src/"
alias sc-validate-security="poetry run bandit -r src/ && poetry run safety check"
alias sc-validate-tests="poetry run pytest --cov=src --cov-fail-under=90"
alias sc-validate-performance="/analyze performance --focus bottlenecks --uc"
alias sc-validate-docs="/document validation --persona-scribe --uc"
```

## Project-Specific Optimizations

### ECAP Domain Mappings
- **src/data_ingestion/**: Use `--persona-backend` with Kafka focus
- **src/analytics/**: Use `--persona-analyzer` with Spark focus
- **src/api/**: Use `--persona-backend` with FastAPI focus
- **src/dashboard/**: Use `--persona-frontend` with Streamlit focus
- **terraform/**: Use `--persona-devops` with infrastructure focus
- **docs/**: Use `--persona-scribe` with technical writing focus

### Context Loading Strategy
```bash
# Load context efficiently based on work area
function sc-load-context() {
    case $1 in
        "kafka")     /load @src/data_ingestion/ --persona-backend --uc ;;
        "spark")     /load @src/analytics/ --persona-analyzer --c7 --uc ;;
        "api")       /load @src/api/ --persona-backend --c7 --uc ;;
        "dashboard") /load @src/dashboard/ --persona-frontend --magic --uc ;;
        "infra")     /load @terraform/ @config/ --persona-devops --uc ;;
        "all")       /load @src/ --delegate auto --uc ;;
        *)           echo "Usage: sc-load-context {kafka|spark|api|dashboard|infra|all}" ;;
    esac
}
```
