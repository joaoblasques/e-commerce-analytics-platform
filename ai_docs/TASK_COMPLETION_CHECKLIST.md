# Task Completion Checklist

## 🚨 MANDATORY CHECKLIST - READ BEFORE EVERY TASK COMPLETION

This checklist must be followed for **EVERY** task completion to ensure proper documentation and tracking.

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
     - [ ] **Reference GitHub issue** in task documentation
   - [ ] **Only proceed** when CI/CD is green OR technical debt is properly documented

5. **✅ DOCUMENTATION UPDATE (CRITICAL)**
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

6. **✅ Final Verification**
   - [ ] CI/CD pipeline is green (or technical debt documented)
   - [ ] Both documentation files committed
   - [ ] All files pushed to master
   - [ ] Task completion confirmed

### 🔥 REMINDER SYSTEM

**NEVER skip the documentation update step!**
**NEVER consider a task complete with failing CI/CD!**

The human will ask about missing documentation updates or failing CI/CD if you forget. To prevent this:

1. **Always monitor CI/CD after PR merge** - failing CI/CD means task is NOT complete
2. **Always update both files immediately after PR merge**
3. **Use the TodoWrite tool to track documentation as a separate task**
4. **Create GitHub issues for CI/CD technical debt** when immediate fixes aren't feasible
5. **Double-check both documentation files AND CI/CD status before declaring task complete**

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

#### PR Merge Command (with branch deletion):
```bash
gh pr merge PR_NUMBER --squash --delete-branch
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
- **CI/CD pipeline passes completely (or technical debt is properly documented)**
- Both documentation files are updated
- All changes are committed to master
- Statistics are updated correctly

### 💡 Pro Tips

1. **Monitor CI/CD immediately** after PR merge - don't wait or assume it passes
2. **Always use `--delete-branch`** when merging PRs to automatically clean up feature branches
3. **Use TodoWrite** to track documentation updates as separate tasks
4. **Always read this checklist** before starting any task
5. **Double-check documentation AND CI/CD** before declaring completion
6. **Keep templates handy** for consistent formatting
7. **Update statistics** (total tasks count) in execution summary
8. **Create GitHub issues proactively** for technical debt to maintain development velocity
9. **Verify branch deletion** - feature branches should not accumulate on remote

---

**This checklist exists because:**
- Documentation updates were forgotten multiple times
- Tasks were considered complete with failing CI/CD pipelines  
- Feature branches were not deleted after PR merges, causing branch accumulation
- PR workflow was occasionally bypassed, leading to process violations
- Following it ensures consistent project tracking and prevents human frustration with missing updates, broken builds, or messy git history
