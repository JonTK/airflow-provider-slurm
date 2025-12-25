"""Custom exceptions for the Slurm executor."""


class SlurmExecutorException(Exception):
    """Base exception for all Slurm executor errors."""
    pass


class SlurmTokenError(SlurmExecutorException):
    """Raised when token generation or validation fails."""
    pass


class SlurmAPIError(SlurmExecutorException):
    """Raised when Slurm REST API requests fail."""
    
    def __init__(self, message: str, status_code: int = None, response_text: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class SlurmConfigurationError(SlurmExecutorException):
    """Raised when configuration is invalid or missing."""
    pass


class SlurmJobSubmissionError(SlurmExecutorException):
    """Raised when job submission fails."""
    pass


class SlurmJobNotFoundError(SlurmExecutorException):
    """Raised when a job cannot be found in Slurm."""
    pass