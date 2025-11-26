# Conventional Commits

This project follows the [Conventional Commits](https://www.conventionalcommits.org/) specification for commit messages. This provides a standardized format that makes it easier to understand the project history and automate versioning.

## Commit Message Structure

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Components

- **type**: The kind of change being made (see types below)
- **scope**: Optional context for the change (e.g., `protocol`, `server`, `logtree`)
- **description**: A brief summary of the change in present tense
- **body**: Optional detailed explanation of the change
- **footer**: Optional metadata (breaking changes, issue references, etc.)

## Commit Types

### `feat` - New Features

A commit that adds new functionality or capabilities.

**When to use:**

- Adding a new command or feature
- Implementing new API endpoints
- Adding new configuration options

**Examples:**

```
feat(protocol): add XREAD command support
feat(logtree): implement range query optimization
feat: add connection pooling
```

### `fix` - Bug Fixes

A commit that patches a bug in the codebase.

**When to use:**

- Fixing incorrect behavior
- Resolving edge cases
- Correcting logic errors

**Examples:**

```
fix(server): resolve race condition in connection handler
fix(protocol): correct integer parsing for negative values
fix: prevent memory leak in buffer pool
```

### `docs` - Documentation

Changes to documentation only, no code changes.

**When to use:**

- Updating README files
- Adding or modifying code comments
- Writing or updating specifications
- Improving inline documentation

**Examples:**

```
docs: update installation instructions
docs(protocol): add examples for XADD command
docs: fix typos in SPECIFICATION.md
```

### `style` - Code Style

Changes that don't affect the meaning of the code.

**When to use:**

- Formatting changes (whitespace, indentation)
- Adding or removing semicolons
- Code style adjustments
- Linting fixes that don't change behavior

**Examples:**

```
style: format code with gofmt
style(handler): improve variable naming consistency
style: remove trailing whitespace
```

### `refactor` - Code Refactoring

Code changes that neither fix bugs nor add features.

**When to use:**

- Restructuring existing code
- Improving code organization
- Simplifying complex functions
- Renaming for clarity (without changing behavior)

**Examples:**

```
refactor(server): extract connection logic into separate function
refactor: simplify error handling in decoder
refactor(logtree): optimize tree traversal algorithm
```

### `perf` - Performance Improvements

Changes that improve performance.

**When to use:**

- Optimizing algorithms
- Reducing memory usage
- Improving response times
- Caching improvements

**Examples:**

```
perf(logtree): use binary search for key lookup
perf: reduce allocations in command decoder
perf(bufferpool): implement zero-copy buffer reuse
```

### `test` - Tests

Adding or modifying tests.

**When to use:**

- Adding new test cases
- Updating existing tests
- Adding benchmark tests
- Fixing broken tests

**Examples:**

```
test(protocol): add fuzzing tests for decoder
test: increase coverage for error cases
test(server): add integration tests for connection pool
```

### `build` - Build System

Changes to the build system or external dependencies.

**When to use:**

- Updating dependencies in go.mod
- Modifying Makefile
- Changing build scripts
- Updating compiler flags

**Examples:**

```
build: update Go version to 1.21
build: add new dependency for metrics collection
build(deps): bump golang.org/x/net to v0.20.0
```

### `ci` - Continuous Integration

Changes to CI configuration and scripts.

**When to use:**

- Modifying GitHub Actions workflows
- Updating CI pipelines
- Changing automated test configurations

**Examples:**

```
ci: add automated benchmarking workflow
ci: enable race detector in test suite
ci: update Go versions in test matrix
```

### `chore` - Maintenance

Other changes that don't modify source or test files.

**When to use:**

- Updating .gitignore
- Modifying editor configurations
- Routine maintenance tasks
- Updating tool configurations

**Examples:**

```
chore: update .gitignore for IDE files
chore: clean up temporary test files
chore: update copyright year
```

### `revert` - Reverts

Reverting a previous commit.

**When to use:**

- Undoing a previous commit
- Rolling back changes that caused issues

**Examples:**

```
revert: "feat(protocol): add XREAD command support"
revert: rollback connection pool changes
```

## Breaking Changes

Breaking changes should be indicated by adding `!` after the type/scope or by including a `BREAKING CHANGE:` footer.

**Examples:**

```
feat(protocol)!: change command encoding format

BREAKING CHANGE: The protocol now uses a different wire format.
Clients must be updated to version 0.2.0 or later.
```

```
fix!: remove deprecated buffer allocation method

BREAKING CHANGE: BufferPool.Get() now requires a size parameter
```

## Scope Guidelines

Use scopes to indicate which part of the codebase is affected:

- `protocol`: Changes to protocol encoding/decoding
- `server`: Server and connection handling
- `logtree`: Log-structured tree implementation
- `handler`: Command handlers
- `config`: Configuration management
- `service`: Service layer components
- `bufferpool`: Buffer pool management

## Examples of Good Commit Messages

### Simple commit

```
feat(protocol): add support for XLEN command
```

### Commit with body

```
fix(server): prevent goroutine leak on connection close

The connection handler was not properly canceling the read loop
when clients disconnected, causing goroutines to accumulate.
Added proper context cancellation and cleanup.
```

### Commit with footer

```
feat(logtree): implement snapshotting mechanism

Add periodic snapshot creation to reduce recovery time after
crashes. Snapshots are written to disk every 10,000 operations.

Closes #42
```

### Breaking change

```
refactor(protocol)!: change reply format to match Redis RESP3

BREAKING CHANGE: The reply format has been updated to be compatible
with RESP3. Clients using the old format will need to be updated.

Migration guide available at docs/migration-v0.2.md
```

## Best Practices

1. **Use present tense**: "add feature" not "added feature"
2. **Keep descriptions concise**: Under 50 characters if possible
3. **Use the body for context**: Explain the "why" not just the "what"
4. **Reference issues**: Use footers to link to issue tracker
5. **One logical change per commit**: Keep commits focused and atomic
6. **Test before committing**: Ensure tests pass with your changes

## Tools

You can use commit message linting tools to enforce these conventions:

```bash
# Install commitlint (optional)
npm install -g @commitlint/cli @commitlint/config-conventional
```

## References

- [Conventional Commits Specification](https://www.conventionalcommits.org/)
- [Angular Convention](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit)
- [Semantic Versioning](https://semver.org/)
