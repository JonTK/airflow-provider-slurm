"""Helper utilities for testing against real Slurm clusters.

These utilities enable integration testing against real clusters while
gracefully falling back when clusters are unavailable.
"""

import logging
import os
import re
import socket
import subprocess
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_cluster_config() -> Dict[str, str]:
    """Get cluster configuration from environment variables.

    Returns:
        Dictionary with cluster configuration

    Environment Variables:
        SLURM_TEST_CLUSTER_HOST: Hostname (default: rocky9.ar.jontk.com)
        SLURM_TEST_CLUSTER_PORT: Port (default: 6820)
        SLURM_TEST_CLUSTER_USER: SSH username (default: root)
        SLURM_TEST_SSH_KEY: Path to SSH key (default: ~/.ssh/id_rsa)
    """
    return {
        "host": os.getenv("SLURM_TEST_CLUSTER_HOST", "rocky9.ar.jontk.com"),
        "port": os.getenv("SLURM_TEST_CLUSTER_PORT", "6820"),
        "user": os.getenv("SLURM_TEST_CLUSTER_USER", "root"),
        "ssh_key": os.getenv("SLURM_TEST_SSH_KEY", os.path.expanduser("~/.ssh/id_rsa")),
    }


def can_reach_cluster(host: str, port: int, timeout: int = 5) -> bool:
    """Check if cluster host is reachable.

    Args:
        host: Hostname to check
        port: Port to check
        timeout: Connection timeout in seconds

    Returns:
        True if host is reachable
    """
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        logger.info(f"Successfully reached {host}:{port}")
        return True
    except (socket.timeout, socket.error, OSError) as e:
        logger.debug(f"Cannot reach {host}:{port}: {e}")
        return False


def fetch_token_via_ssh(
    host: str, user: str, ssh_key: Optional[str] = None, lifespan: int = 3600
) -> Optional[str]:
    """Fetch Slurm JWT token via SSH.

    Args:
        host: Remote hostname
        user: SSH username
        ssh_key: Path to SSH private key (optional)
        lifespan: Token lifespan in seconds

    Returns:
        JWT token string or None if failed
    """
    try:
        # Build SSH command
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=5"]

        if ssh_key and os.path.exists(ssh_key):
            ssh_cmd.extend(["-i", ssh_key])

        ssh_cmd.extend([f"{user}@{host}", f"scontrol token lifespan={lifespan}"])

        # Execute SSH command
        logger.info(f"Fetching token from {user}@{host}")
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            logger.error(f"SSH command failed: {result.stderr}")
            return None

        # Parse token from output
        # Expected format: SLURM_JWT=eyJhbGc...
        output = result.stdout.strip()
        match = re.search(r"SLURM_JWT=(\S+)", output)

        if match:
            token = match.group(1)
            logger.info(f"Successfully fetched token: {token[:20]}...")
            return token
        else:
            logger.error(f"Could not parse token from output: {output}")
            return None

    except subprocess.TimeoutExpired:
        logger.error("SSH command timed out")
        return None
    except Exception as e:
        logger.error(f"Failed to fetch token via SSH: {e}")
        return None


def is_cluster_available(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    ssh_key: Optional[str] = None,
) -> bool:
    """Check if Slurm cluster is available for testing.

    This function checks:
    1. Network connectivity to cluster
    2. Ability to fetch token via SSH

    Args:
        host: Cluster hostname (uses env var if not provided)
        port: Cluster port (uses env var if not provided)
        user: SSH username (uses env var if not provided)
        ssh_key: SSH key path (uses env var if not provided)

    Returns:
        True if cluster is available and accessible
    """
    config = get_cluster_config()
    host = host or config["host"]
    port = int(port or config["port"])
    user = user or config["user"]
    ssh_key = ssh_key or config["ssh_key"]

    # Check if explicitly disabled
    if os.getenv("SKIP_REAL_CLUSTER_TESTS", "").lower() in ("1", "true", "yes"):
        logger.info("Real cluster tests explicitly disabled via SKIP_REAL_CLUSTER_TESTS")
        return False

    # Check network connectivity
    if not can_reach_cluster(host, port):
        logger.info(f"Cluster {host}:{port} not reachable")
        return False

    # Try to fetch token
    token = fetch_token_via_ssh(host, user, ssh_key)
    if not token:
        logger.info("Cannot fetch token from cluster")
        return False

    logger.info(f"Cluster {host} is available for testing")
    return True
