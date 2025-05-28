"""
Token class for authentication tokens with expiry handling.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional


class Token:
    """
    Represents an OAuth token with expiry information.

    This class handles token state including expiry calculation.
    """

    # Minimum time buffer before expiry to consider a token still valid (in seconds)
    MIN_VALIDITY_BUFFER = 10

    def __init__(
        self,
        access_token: str,
        token_type: str,
        refresh_token: str = "",
        expiry: Optional[datetime] = None,
    ):
        """
        Initialize a Token object.

        Args:
            access_token: The access token string
            token_type: The token type (usually "Bearer")
            refresh_token: Optional refresh token
            expiry: Token expiry datetime, must be provided

        Raises:
            ValueError: If no expiry is provided
        """
        self.access_token = access_token
        self.token_type = token_type
        self.refresh_token = refresh_token

        # Ensure we have an expiry time
        if expiry is None:
            raise ValueError("Token expiry must be provided")

        # Ensure expiry is timezone-aware
        if expiry.tzinfo is None:
            # Convert naive datetime to aware datetime
            self.expiry = expiry.replace(tzinfo=timezone.utc)
        else:
            self.expiry = expiry

    def is_valid(self) -> bool:
        """
        Check if the token is valid (has at least MIN_VALIDITY_BUFFER seconds before expiry).

        Returns:
            bool: True if the token is valid, False otherwise
        """
        buffer = timedelta(seconds=self.MIN_VALIDITY_BUFFER)
        return datetime.now(tz=timezone.utc) + buffer < self.expiry

    def __str__(self) -> str:
        """Return the token as a string in the format used for Authorization headers."""
        return f"{self.token_type} {self.access_token}"
