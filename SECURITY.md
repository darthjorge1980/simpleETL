# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 1.x     | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in SimpleETL, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, send an email to: **jorge.lozano.meza@gmail.com**

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

You will receive an acknowledgment within 48 hours. We will work with you to understand and address the issue before any public disclosure.

## Security Considerations

SimpleETL allows users to build and execute data pipelines. Be aware of the following:

### Expression Evaluation

Some plugins (Add Column, Filter Rows, If/Then/Else) use `eval()` to execute user-provided Polars expressions. **Only run SimpleETL in trusted environments** where the users crafting pipelines are trusted. Do not expose the API to the public internet without authentication.

### File System Access

Source and destination plugins read and write files based on user-provided paths. Ensure the application runs with appropriate file system permissions and restrict access to sensitive directories.

### SQL Plugin

The SQL Reader plugin accepts connection strings and queries. Never expose this to untrusted users, as it could allow arbitrary database access.

### API Reader

The API Reader plugin makes HTTP requests to user-specified URLs. In a shared environment, this could be used for SSRF (Server-Side Request Forgery). Restrict network access if needed.

## Recommendations

- Run SimpleETL behind a reverse proxy with authentication (e.g., nginx + OAuth)
- Use environment variables for sensitive connection strings instead of hardcoding them in pipelines
- Restrict file system access using OS-level permissions or containers
- Do not expose port 8080 directly to the internet
- Review generated Python scripts before running them in production
