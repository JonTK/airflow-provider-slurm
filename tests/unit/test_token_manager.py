"""Unit tests for SlurmTokenManager."""

import subprocess
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from airflow_slurm_executor.exceptions import SlurmTokenError
from airflow_slurm_executor.slurm_token_manager import SlurmTokenManager


class TestSlurmTokenManager:
    """Test cases for SlurmTokenManager."""

    def test_init_default_username(self):
        """Test initialization with default username."""
        with patch("getpass.getuser", return_value="testuser"):
            manager = SlurmTokenManager()
            assert manager.username == "testuser"
            assert manager.lifespan == 3600
            assert manager.token is None
            assert manager.token_expiry is None

    def test_init_custom_params(self):
        """Test initialization with custom parameters."""
        manager = SlurmTokenManager(
            username="customuser",
            lifespan=7200,
            scontrol_path="/usr/bin/scontrol",
        )
        assert manager.username == "customuser"
        assert manager.lifespan == 7200
        assert manager.scontrol_path == "/usr/bin/scontrol"

    def test_token_generation_success(self):
        """Test successful token generation."""
        mock_result = MagicMock()
        mock_result.stdout = "SLURM_JWT=test_jwt_token_12345"
        
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            manager = SlurmTokenManager(username="testuser")
            token = manager.get_token()
            
            assert token == "test_jwt_token_12345"
            assert manager.token == "test_jwt_token_12345"
            assert manager.token_expiry is not None
            
            # Verify subprocess call
            mock_run.assert_called_once_with(
                ["scontrol", "token", "lifespan=3600", "username=testuser"],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )

    def test_token_caching(self):
        """Test that tokens are cached and reused."""
        mock_result = MagicMock()
        mock_result.stdout = "SLURM_JWT=cached_token"
        
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            manager = SlurmTokenManager()
            
            # First call should generate token
            token1 = manager.get_token()
            assert mock_run.call_count == 1
            
            # Second call should use cached token
            token2 = manager.get_token()
            assert token1 == token2
            assert mock_run.call_count == 1  # No additional call

    def test_token_refresh_on_expiry(self):
        """Test token refresh when close to expiry."""
        mock_result1 = MagicMock()
        mock_result1.stdout = "SLURM_JWT=token1"
        
        mock_result2 = MagicMock()
        mock_result2.stdout = "SLURM_JWT=token2"
        
        with patch("subprocess.run", side_effect=[mock_result1, mock_result2]) as mock_run:
            manager = SlurmTokenManager(lifespan=300)  # 5 minutes
            
            # First token
            token1 = manager.get_token()
            assert token1 == "token1"
            
            # Force expiry by setting token_expiry to past
            manager.token_expiry = datetime.now() + timedelta(minutes=4)
            
            # Should generate new token
            token2 = manager.get_token()
            assert token2 == "token2"
            assert mock_run.call_count == 2

    def test_token_generation_failure(self):
        """Test handling of token generation failure."""
        with patch("subprocess.run", side_effect=subprocess.CalledProcessError(
            1, ["scontrol"], stderr="Authentication failed"
        )):
            manager = SlurmTokenManager()
            
            with pytest.raises(SlurmTokenError) as exc_info:
                manager.get_token()
            
            assert "Token generation failed" in str(exc_info.value)
            assert "Authentication failed" in str(exc_info.value)

    def test_token_generation_timeout(self):
        """Test handling of token generation timeout."""
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(
            ["scontrol"], 10
        )):
            manager = SlurmTokenManager()
            
            with pytest.raises(SlurmTokenError) as exc_info:
                manager.get_token()
            
            assert "timed out" in str(exc_info.value)

    def test_unexpected_output_format(self):
        """Test handling of unexpected scontrol output."""
        mock_result = MagicMock()
        mock_result.stdout = "UNEXPECTED_OUTPUT"
        
        with patch("subprocess.run", return_value=mock_result):
            manager = SlurmTokenManager()
            
            with pytest.raises(SlurmTokenError) as exc_info:
                manager.get_token()
            
            assert "Unexpected scontrol output format" in str(exc_info.value)

    def test_invalidate_token(self):
        """Test token invalidation."""
        mock_result = MagicMock()
        mock_result.stdout = "SLURM_JWT=test_token"
        
        with patch("subprocess.run", return_value=mock_result):
            manager = SlurmTokenManager()
            
            # Generate token
            token = manager.get_token()
            assert manager.token is not None
            assert manager.token_expiry is not None
            
            # Invalidate
            manager.invalidate()
            assert manager.token is None
            assert manager.token_expiry is None

    def test_token_without_username(self):
        """Test token generation without specifying username."""
        mock_result = MagicMock()
        mock_result.stdout = "SLURM_JWT=test_token"
        
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            with patch("getpass.getuser", return_value="current_user"):
                manager = SlurmTokenManager(username=None)
                token = manager.get_token()
                
                # Should not include username parameter
                cmd_args = mock_run.call_args[0][0]
                assert "username=current_user" in cmd_args

    def test_custom_scontrol_path(self):
        """Test using custom scontrol path."""
        mock_result = MagicMock()
        mock_result.stdout = "SLURM_JWT=test_token"
        
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            manager = SlurmTokenManager(scontrol_path="/custom/path/scontrol")
            manager.get_token()
            
            # Verify custom path was used
            cmd_args = mock_run.call_args[0][0]
            assert cmd_args[0] == "/custom/path/scontrol"