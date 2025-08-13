import base64
import logging
from typing import Dict, Optional, Tuple
import socket

logger = logging.getLogger(__name__)

try:
    import kerberos
    KERBEROS_AVAILABLE = True
except ImportError:
    try:
        import winkerberos as kerberos  # Windows fallback
        KERBEROS_AVAILABLE = True
    except ImportError:
        KERBEROS_AVAILABLE = False
        kerberos = None


class KerberosProxyAuth:
    """
    Handles Kerberos authentication for HTTP proxies.
    
    This class manages the GSSAPI negotiation process required for
    Kerberos authentication with HTTP proxies.
    """
    
    def __init__(
        self,
        service_name: str = "HTTP",
        principal: Optional[str] = None,
        gssflags: int = None,
        delegate: bool = False,
        mutual_authentication: int = 1,  # REQUIRED
    ):
        if not KERBEROS_AVAILABLE:
            raise ImportError(
                "Kerberos proxy authentication requires 'pykerberos' (Linux/Mac) "
                "or 'winkerberos' (Windows). Install with: pip install databricks-sql-connector[kerberos]"
            )
        
        self.service_name = service_name
        self.principal = principal
        
        # Set up GSS flags
        if gssflags is None:
            self.gssflags = (
                kerberos.GSS_C_MUTUAL_FLAG | 
                kerberos.GSS_C_SEQUENCE_FLAG
            )
        else:
            self.gssflags = gssflags
            
        if delegate:
            self.gssflags |= kerberos.GSS_C_DELEG_FLAG
            
        self.mutual_authentication = mutual_authentication
        self._context = None
        
    def generate_proxy_auth_header(self, proxy_host: str) -> Dict[str, str]:
        """
        Generate Proxy-Authorization header for Kerberos authentication.
        
        Args:
            proxy_host: The proxy hostname
            
        Returns:
            Dict containing the Proxy-Authorization header
        """
        # Build the service principal name (SPN)
        # Format: HTTP@proxy.example.com
        service = f"{self.service_name}@{proxy_host}"
        
        try:
            # Initialize GSSAPI context
            if self.principal:
                result, self._context = kerberos.authGSSClientInit(
                    service,
                    principal=self.principal,
                    gssflags=self.gssflags
                )
            else:
                # Use default credentials
                result, self._context = kerberos.authGSSClientInit(
                    service,
                    gssflags=self.gssflags
                )
            
            if result < 1:
                raise Exception(f"GSS client init failed with result {result}")
            
            # Get the initial client token
            result = kerberos.authGSSClientStep(self._context, "")
            
            if result < 0:
                raise Exception(f"GSS client step failed with result {result}")
                
            # Get the base64-encoded token
            token = kerberos.authGSSClientResponse(self._context)
            
            return {"Proxy-Authorization": f"Negotiate {token}"}
            
        except kerberos.GSSError as e:
            logger.error(f"Kerberos authentication failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Kerberos auth: {e}")
            raise
            
    def handle_407_response(self, auth_header: str) -> Optional[Dict[str, str]]:
        """
        Handle 407 Proxy Authentication Required response.
        
        Args:
            auth_header: The Proxy-Authenticate header value from 407 response
            
        Returns:
            Updated headers if mutual auth is required, None otherwise
        """
        if not self._context:
            raise Exception("No Kerberos context available")
            
        if not auth_header.startswith("Negotiate "):
            return None
            
        # Extract the server's token
        server_token = auth_header[10:]  # Skip "Negotiate "
        
        try:
            # Continue GSSAPI negotiation
            result = kerberos.authGSSClientStep(self._context, server_token)
            
            if result < 0:
                raise Exception(f"GSS client step failed with result {result}")
                
            # Check if mutual authentication succeeded
            if self.mutual_authentication == 1:  # REQUIRED
                # Get the status of the negotiation
                # Note: Some kerberos implementations might not have this method
                if hasattr(kerberos, 'authGSSClientStepResult'):
                    result = kerberos.authGSSClientStepResult(self._context)
                    if result != kerberos.AUTH_GSS_COMPLETE:
                        raise Exception("Mutual authentication failed")
                    
            # Get response token if any
            token = kerberos.authGSSClientResponse(self._context)
            if token:
                return {"Proxy-Authorization": f"Negotiate {token}"}
                
        except Exception as e:
            logger.error(f"Error handling 407 response: {e}")
            raise
            
        return None
        
    def cleanup(self):
        """Clean up Kerberos context"""
        if self._context:
            try:
                kerberos.authGSSClientClean(self._context)
            except Exception:
                pass
            self._context = None
            
    def __del__(self):
        self.cleanup()