# Assistant Rules for Azure EventHubs Integration Tests

## CRITICAL RULES - MUST FOLLOW

### 1. File Analysis Before Changes
- **NEVER** blindly apply the same change to multiple files
- **ALWAYS** read and analyze each file individually before making changes
- **VERIFY** that a change is actually needed in each specific file
- **UNDERSTAND** the context and purpose of each file before modifying it

### 2. Change Validation Process
Before making any change to a file:
1. Read the entire file to understand its structure and purpose
2. Identify the specific problem that needs to be solved
3. Verify that the proposed change actually addresses that problem
4. Check if the change is appropriate for that specific file's context
5. Only then implement the change

### 3. Multi-File Changes
When a change needs to be applied to multiple files:
1. Analyze each file individually first
2. Determine if each file actually needs the change
3. Make changes one file at a time
4. Test each change before moving to the next file
5. Document why each file needs the specific change

### 4. Error Prevention
- **NEVER** assume all files have the same structure or need the same changes
- **NEVER** copy-paste changes between files without verification
- **ALWAYS** understand the difference between similar files before making changes
- **VERIFY** that changes are correct for each specific context

### 5. Testing Strategy
- Test changes incrementally, one file at a time
- Verify that each change works before making additional changes
- Don't make multiple changes simultaneously unless absolutely necessary
- Document the reasoning behind each change

## Examples of BAD Practices (DO NOT DO)
- Blindly adding `[TearDown]` methods to all test files without checking if they need them
- Copy-pasting the same code changes across multiple files
- Making assumptions about file structure without reading them first
- Applying "fixes" to files that don't have the problem being fixed

## Examples of GOOD Practices (DO THIS)
- Read each file completely before making changes
- Understand the specific problem in each file
- Verify that the proposed solution actually fixes the problem
- Test each change individually
- Document the reasoning for each change
