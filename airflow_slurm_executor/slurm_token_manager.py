"""Slurm token management for authentication."""

import getpass
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Optional

from airflow_slurm_executor.exceptions import SlurmTokenError

logger = logging.getLogger(__name__)


class SlurmTokenManager:
    """Manages Slurm JWT tokens for API authentication.
    
    This class handles:
    - Token generation via scontrol
    - Token caching to minimize generation calls
    - Automatic token refresh before expiration
    """

    def __init__(
        self,
        username: Optional[str] = None,
        lifespan: int = 3600,
        scontrol_path: str = "scontrol",
    ) -> None:
        """Initialize the token manager.
        
        Args:
            username: Slurm username. Defaults to current system user.
            lifespan: Token lifespan in seconds. Default 3600 (1 hour).
            scontrol_path: Path to scontrol binary. Default assumes it's in PATH.
        """
        self.username = username or getpass.getuser()
        self.lifespan = lifespan
        self.scontrol_path = scontrol_path
        self.token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
        
        logger.info(
            f"Initialized SlurmTokenManager for user {self.username} "
            f"with token lifespan {lifespan}s"
        )

    def get_token(self) -> str:
        """Get a valid JWT token, refreshing if necessary.
        
        Returns:
            Valid JWT token string
            
        Raises:
            SlurmTokenError: If token generation fails
        """
        if self._token_is_valid():
            logger.debug("Using cached token")
            return self.token  # type: ignore[return-value]
        
        logger.info("Generating new Slurm token")
        return self._fetch_new_token()

    def _token_is_valid(self) -> bool:
        """Check if the cached token is still valid.
        
        Returns:
            True if token exists and won't expire in the next 5 minutes
        """
        if not self.token or not self.token_expiry:
            return False
        
        # Refresh 5 minutes before expiry to avoid edge cases
        buffer = timedelta(minutes=5)
        return datetime.now() < (self.token_expiry - buffer)

    def _fetch_new_token(self) -> str:
        """Generate a new token using scontrol.
        
        Returns:
            New JWT token string
            
        Raises:
            SlurmTokenError: If token generation fails
        """
        cmd = [self.scontrol_path, "token", f"lifespan={self.lifespan}"]
        
        if self.username:
            cmd.append(f"username={self.username}")
        
        logger.debug(f"Running command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
            
            # Parse output: "SLURM_JWT=token_string"
            output = result.stdout.strip()
            
            if not output.startswith("SLURM_JWT="):
                raise SlurmTokenError(
                    f"Unexpected scontrol output format: {output}"
                )
            
            self.token = output.split("=", 1)[1]
            self.token_expiry = datetime.now() + timedelta(seconds=self.lifespan)
            
            logger.info(
                f"Generated new token for user {self.username}, "
                f"expires at {self.token_expiry}"
            )
            
            return self.token
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Token generation failed: {e.stderr or e}"
            logger.error(error_msg)
            raise SlurmTokenError(error_msg) from e
            
        except subprocess.TimeoutExpired as e:
            error_msg = "Token generation timed out after 10 seconds"
            logger.error(error_msg)
            raise SlurmTokenError(error_msg) from e
            
        except Exception as e:
            error_msg = f"Unexpected error during token generation: {e}"
            logger.error(error_msg)
            raise SlurmTokenError(error_msg) from e

    def invalidate(self) -> None:
        """Invalidate the cached token, forcing refresh on next request."""
        logger.debug("Invalidating cached token")
        self.token = None
        self.token_expiry = None