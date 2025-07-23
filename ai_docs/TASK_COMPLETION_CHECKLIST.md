# Task Completion Checklist

## 🚨 MANDATORY CHECKLIST - READ BEFORE EVERY TASK START AND COMPLETION

This checklist must be followed for **EVERY** task start and completion to ensure proper documentation and tracking.

### 📖 PRE-TASK REQUIREMENT (MANDATORY)

**Before starting ANY task, you MUST:**

- [ ] **Read ALL documentation in ai_docs/ folder**
  - [ ] Read CLAUDE.md for project guidelines and principles
  - [ ] Read ECAP_prd.md for project requirements
  - [ ] Read ECAP_tasklist.md for current task details
  - [ ] Read ECAP_execution_summary.md for project status
  - [ ] Read ECAP_planning.md for architectural context
  - [ ] Read this TASK_COMPLETION_CHECKLIST.md for process requirements
  - [ ] Read CI_CD_STABILITY_PLAN.md for CI/CD best practices

**This ensures you have complete context and understanding before beginning any work.**

### ✅ Task Completion Workflow

**Before marking any task as complete, ensure ALL these steps are done:**

1. **✅ Code Implementation**
   - [ ] All acceptance criteria met
   - [ ] Code follows project standards
   - [ ] Tests written and passing
   - [ ] Documentation created

2. **✅ Git Workflow**
   - [ ] Feature branch created
   - [ ] Changes committed with conventional format
   - [ ] Branch pushed to remote
   - [ ] Pull request created

3. **✅ Quality Assurance & PR Merge**
   - [ ] All tests passing
   - [ ] Pre-commit hooks passing
   - [ ] Code reviewed (if applicable)
   - [ ] PR merged successfully
   - [ ] **Feature branch deleted** (automatic with `--delete-branch` flag)

4. **✅ CI/CD MONITORING (MANDATORY)**
   - [ ] **Monitor GitHub Actions workflow** after PR merge
   - [ ] **Verify all CI/CD checks pass** (tests, linting, security scans)
   - [ ] **If any CI/CD errors occur:**
     - [ ] **STOP** - Task is NOT complete with failing CI/CD
     - [ ] **Option 1: Fix errors immediately** and re-run checks
     - [ ] **Option 2: Create GitHub issue** for technical debt if:
       - Errors are non-critical and don't affect functionality
       - Fixing would require significant time/scope change
       - Errors are infrastructure/tooling related
     - [ ] **CRITICAL: When postponing as technical debt**:
       - [ ] **Fix CI/CD workflow** to handle the errors gracefully (continue-on-error, fallbacks)
       - [ ] **Ensure pipeline shows GREEN** - failing CI/CD is never acceptable
       - [ ] **Document workarounds** in the GitHub issue with specific implementation details
       - [ ] **Reference GitHub issue** in task documentation
   - [ ] **Only proceed** when CI/CD is GREEN (passing) with technical debt properly documented

5. **✅ POST-MERGE CLEANUP (MANDATORY)**
   - [ ] **Pull latest changes from master** after PR merge
   - [ ] **Delete local feature branch**: `git branch -d feature/task-X.X.X`
   - [ ] **Verify remote branch deletion**: Automatic with `--delete-branch` flag
   - [ ] **Clean up ALL merged remote branches**: If any merged feature branches still exist on remote, delete them manually:
     - [ ] Check for stale remote branches: `gh api repos/OWNER/REPO/branches --jq '.[] | select(.name | startswith("feature/")) | .name'`
     - [ ] Verify branches are merged: `gh pr list --state merged --json headRefName`
     - [ ] Delete merged remote branches: `git push origin --delete feature/branch-name`
   - [ ] **Confirm complete branch cleanup**: Use `git branch -a` and verify no stale feature branches remain

6. **✅ DOCUMENTATION UPDATE (CRITICAL)**
   - [ ] **ECAP_tasklist.md** updated with:
     - [ ] Task marked as `[x]` completed
     - [ ] Actual time recorded
     - [ ] Pull request link added
     - [ ] Completion date added
   - [ ] **ECAP_execution_summary.md** updated with:
     - [ ] Task status marked as ✅ Completed
     - [ ] Comprehensive summary added
     - [ ] Key features documented
     - [ ] Repository status updated
     - [ ] Statistics updated (total tasks count)
     - [ ] Next task updated

7. **✅ Final Verification**
   - [ ] CI/CD pipeline is green (or technical debt documented)
   - [ ] Feature branches cleaned up (local and remote)
   - [ ] Both documentation files committed
   - [ ] All files pushed to master
   - [ ] Task completion confirmed

### 🔥 REMINDER SYSTEM

**NEVER skip the documentation update step!**
**NEVER consider a task complete with failing CI/CD!**

The human will ask about missing documentation updates or failing CI/CD if you forget. To prevent this:

1. **Always monitor CI/CD after PR merge** - failing CI/CD means task is NOT complete
2. **Always clean up feature branches after PR merge** - both local and remote
3. **Always update both files immediately after PR merge**
4. **Use the TodoWrite tool to track documentation as a separate task**
5. **Create GitHub issues for CI/CD technical debt** when immediate fixes aren't feasible
6. **Double-check both documentation files AND CI/CD status before declaring task complete**

### 📝 Template for Documentation Updates

#### For ECAP_tasklist.md:
```markdown
- [x] **Task X.X.X**: Task description
  - [x] All sub-tasks completed
  - **Acceptance Criteria**: Description ✅
  - **Estimated Time**: X hours
  - **Actual Time**: X hours X minutes (under/over estimate ✅)
  - **Completed**: YYYY-MM-DD
  - **Repository**: https://github.com/joaoblasques/e-commerce-analytics-platform
  - **Pull Request**: https://github.com/joaoblasques/e-commerce-analytics-platform/pull/XX (Merged)
```

#### PR Merge Command (with automatic branch deletion):
```bash
gh pr merge PR_NUMBER --squash --delete-branch
```

#### Post-Merge Cleanup Commands:
```bash
# Pull latest changes from master
git pull origin master

# Delete local feature branch
git branch -d feature/task-X.X.X

# Check for stale remote feature branches
gh api repos/joaoblasques/e-commerce-analytics-platform/branches --jq '.[] | select(.name | startswith("feature/")) | .name'

# Verify which branches have been merged
gh pr list --state merged --json headRefName --limit 10

# Delete any merged remote branches that still exist
git push origin --delete feature/branch-name-1
git push origin --delete feature/branch-name-2

# Prune local references to deleted remote branches
git fetch --prune

# Final verification - should show no stale feature branches
git branch -a
```

#### For ECAP_execution_summary.md:
```markdown
#### Task X.X.X: Task description
- **Status**: ✅ Completed
- **Estimated Time**: X hours
- **Actual Time**: X hours X minutes (under/over estimate ✅)
- **Completed**: YYYY-MM-DD
- **Repository**: https://github.com/joaoblasques/e-commerce-analytics-platform
- **Pull Request**: https://github.com/joaoblasques/e-commerce-analytics-platform/pull/XX (Merged)

**Summary**: [Detailed summary of what was accomplished]

**✅ Task X.X.X Completed: [Task Title]**

**🎯 What Was Delivered**
[Numbered list of deliverables]

**🔧 Key Features Implemented**
[Bullet points of key features]

**📊 Repository Status**
[Statistics and metrics]

**Next up**: Task X.X.X - [Next task description]
```

### 🎯 Success Criteria

A task is only considered complete when:
- All code is implemented and tested
- PR is merged successfully
- **CI/CD pipeline shows GREEN status (passes completely or fails gracefully with documented technical debt)**
- Both documentation files are updated
- All changes are committed to master
- Statistics are updated correctly

**CRITICAL**: CI/CD must NEVER be left in a failing state. If errors are postponed as technical debt, the workflow must be fixed to handle them gracefully and show GREEN status.

### 💡 Pro Tips

1. **Monitor CI/CD immediately** after PR merge - don't wait or assume it passes
2. **Always use `--delete-branch`** when merging PRs to automatically clean up feature branches
3. **GREEN CI/CD is mandatory** - fix workflows with continue-on-error/fallbacks when postponing issues as technical debt
4. **Use TodoWrite** to track documentation updates as separate tasks
5. **Always read this checklist** before starting any task
6. **Double-check documentation AND CI/CD** before declaring completion
7. **Keep templates handy** for consistent formatting
8. **Update statistics** (total tasks count) in execution summary
9. **Create GitHub issues proactively** for technical debt to maintain development velocity
10. **Verify branch deletion** - feature branches should not accumulate on remote

---

**This checklist exists because:**
- Documentation updates were forgotten multiple times
- Tasks were considered complete with failing CI/CD pipelines
- Feature branches were not deleted after PR merges, causing branch accumulation on both local and remote repositories
- Remote feature branches accumulated even when `--delete-branch` was used, requiring manual cleanup
- PR workflow was occasionally bypassed, leading to process violations
- Following it ensures consistent project tracking and prevents human frustration with missing updates, broken builds, or messy git history
