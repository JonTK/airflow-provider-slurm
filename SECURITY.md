# Security Policy

## Supported Versions

We release security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1.0 | :x:                |

As this is an alpha release, we recommend staying on the latest version to receive all security patches.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report security vulnerabilities by emailing the maintainers directly. You can find contact information by checking the repository's commit history or by reaching out through GitHub.

When reporting a vulnerability, please include:

1. **Description**: A clear description of the vulnerability
2. **Impact**: The potential impact and affected components
3. **Reproduction Steps**: Detailed steps to reproduce the issue
4. **Environment**: Version information (Python, Airflow, Slurm, OS)
5. **Proof of Concept**: If applicable, a minimal PoC (please be responsible)
6. **Suggested Fix**: If you have ideas on how to address the issue

### What to Expect

- **Acknowledgment**: We will acknowledge receipt within 48 hours
- **Investigation**: We will investigate and validate the report
- **Updates**: We will provide regular updates on our progress
- **Resolution**: We aim to release a fix within 90 days for confirmed vulnerabilities
- **Credit**: We will credit researchers in security advisories (unless you prefer to remain anonymous)

## Security Considerations

### HPC Environment Security

This provider executes Airflow tasks on HPC clusters via Slurm. Please consider these security aspects:

#### 1. Authentication & Authorization

- **Slurm REST API Authentication**:
  - Use token-based authentication (JWT) for slurmrestd
  - Rotate API tokens regularly
  - Store credentials securely using Airflow Connections with encrypted passwords
  - Never commit credentials to version control

- **Slurm User Permissions**:
  - Run Slurm jobs with appropriate user accounts
  - Use Slurm accounting and QoS policies to limit resource access
  - Implement least-privilege principles for job submission

#### 2. Shared Filesystem Security

- **File Permissions**:
  - Ensure proper file permissions on shared storage (use umask 027 or stricter)
  - Airflow logs and working directories should have restricted access
  - Use separate directories per user/project with appropriate ACLs

- **Sensitive Data**:
  - Never store secrets in DAG files or task logs
  - Use Airflow Variables or Connections for sensitive configuration
  - Consider encrypting data at rest on shared storage

#### 3. Code Execution & Job Isolation

- **DAG Security**:
  - Review all DAG code before deployment
  - Use separate Python virtual environments per project
  - Validate task command inputs to prevent command injection

- **Job Isolation**:
  - Use Slurm cgroups for resource isolation
  - Consider using containers for additional isolation
  - Monitor for privilege escalation attempts

#### 4. Network Security

- **Slurm REST API**:
  - Always use HTTPS for slurmrestd connections
  - Validate SSL/TLS certificates (avoid `verify_ssl=false` in production)
  - Restrict API access via firewall rules
  - Use VPN or private networks for cluster access

- **Airflow Webserver**:
  - Enable Airflow authentication and RBAC
  - Use HTTPS for the webserver
  - Restrict access to authorized users only

#### 5. Logging & Monitoring

- **Audit Trails**:
  - Enable Slurm accounting to track job submissions
  - Monitor Airflow task logs for suspicious activity
  - Set up alerts for failed authentication attempts

- **Log Security**:
  - Ensure logs don't contain sensitive information
  - Implement log rotation and retention policies
  - Restrict access to log directories

### Known Security Limitations

1. **Alpha Status**: This is an alpha release and may contain undiscovered security issues
2. **Slurm API Versions**: Only tested with Slurm 23.11-25.11 (slurmrestd v0.0.40-v0.0.44); other versions may have different security characteristics
3. **Job Recovery**: Job state recovery relies on Slurm job metadata; ensure proper access controls
4. **Command Injection**: While we sanitize inputs, always validate user-provided configuration

## Best Practices

### For Administrators

1. **Keep Software Updated**:
   - Regularly update airflow-provider-slurm
   - Keep Apache Airflow current with security patches
   - Update Slurm to receive security fixes

2. **Harden Configuration**:
   - Use the principle of least privilege
   - Enable all available Slurm security features
   - Configure Airflow with secure defaults

3. **Monitor & Audit**:
   - Implement comprehensive logging
   - Set up security monitoring and alerts
   - Conduct regular security audits

### For DAG Developers

1. **Secure Coding**:
   - Never hardcode credentials in DAGs
   - Validate and sanitize all inputs
   - Use Airflow Connections for external services

2. **Resource Limits**:
   - Always set explicit resource limits (CPU, memory, time)
   - Use appropriate Slurm partitions
   - Implement task timeouts

3. **Error Handling**:
   - Avoid exposing sensitive information in error messages
   - Implement proper exception handling
   - Use task retries appropriately

## Security Updates

Security updates will be announced through:

- GitHub Security Advisories
- Release notes with `[SECURITY]` prefix
- This SECURITY.md file

Subscribe to repository releases to stay informed about security updates.

## Responsible Disclosure

We follow responsible disclosure practices:

1. Security issues are fixed in private before public disclosure
2. We coordinate with reporters on disclosure timelines
3. We publish security advisories with CVE identifiers when applicable
4. We credit security researchers appropriately

## Additional Resources

- [Apache Airflow Security](https://airflow.apache.org/docs/apache-airflow/stable/security/index.html)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE Top 25](https://cwe.mitre.org/top25/)

---

Thank you for helping keep Airflow Provider Slurm secure!
