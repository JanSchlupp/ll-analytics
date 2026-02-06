"""Authentication and session management for Learned League."""

import time
import requests
from typing import Optional

from ..config import Config
from ..logging import get_logger

logger = get_logger(__name__)


class LLSession:
    """
    Manages authenticated sessions with Learned League.

    Usage:
        session = LLSession()
        if session.login():
            html = session.get("/profiles.php?username=someone")
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LL-Analytics/1.0 (Personal analytics tool)"
        })
        self.base_url = Config.LL_BASE_URL
        self.logged_in = False
        self.last_request_time = 0.0

    def _rate_limit(self) -> None:
        """Ensure we don't make requests too quickly."""
        elapsed = time.time() - self.last_request_time
        if elapsed < Config.REQUEST_DELAY:
            time.sleep(Config.REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()

    def login(self, username: Optional[str] = None, password: Optional[str] = None) -> bool:
        """
        Log in to Learned League.

        Args:
            username: LL username (uses config if not provided)
            password: LL password (uses config if not provided)

        Returns:
            True if login successful, False otherwise
        """
        username = username or Config.LL_USERNAME
        password = password or Config.LL_PASSWORD

        if not username or not password:
            raise ValueError("LL credentials not provided. Set LL_USERNAME and LL_PASSWORD in .env")

        login_url = f"{self.base_url}/ucp.php?mode=login"

        # First, get the login page to capture any tokens
        self._rate_limit()
        response = self.session.get(login_url, timeout=Config.REQUEST_TIMEOUT)

        if response.status_code != 200:
            logger.error("Failed to load login page: %s", response.status_code)
            return False

        # Submit login form
        login_data = {
            "username": username,
            "password": password,
            "login": "Login",
            "redirect": "index.php",
        }

        self._rate_limit()
        response = self.session.post(
            login_url,
            data=login_data,
            timeout=Config.REQUEST_TIMEOUT,
            allow_redirects=True
        )

        # Check if login was successful
        # LL typically redirects to index after successful login
        # and shows certain elements only when logged in
        self.logged_in = self._verify_login()

        if self.logged_in:
            logger.info("Successfully logged in as %s", username)
        else:
            logger.error("Login failed. Check credentials.")

        return self.logged_in

    def _verify_login(self) -> bool:
        """Verify that we're actually logged in."""
        self._rate_limit()
        response = self.session.get(
            f"{self.base_url}/index.php",
            timeout=Config.REQUEST_TIMEOUT
        )

        # Look for signs of being logged in
        # This might need adjustment based on actual LL page structure
        return "Logout" in response.text or "ucp.php?mode=logout" in response.text

    def get(self, path: str) -> Optional[str]:
        """
        Make an authenticated GET request.

        Args:
            path: Path relative to base URL (e.g., "/profiles.php?username=X")

        Returns:
            Response HTML text, or None if request failed
        """
        if not self.logged_in:
            logger.warning("Not logged in. Call login() first.")

        url = f"{self.base_url}{path}" if path.startswith("/") else f"{self.base_url}/{path}"

        self._rate_limit()
        try:
            response = self.session.get(url, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error("Request failed for %s: %s", path, e)
            return None

    def logout(self) -> None:
        """Log out and clear session."""
        if self.logged_in:
            self._rate_limit()
            try:
                self.session.get(
                    f"{self.base_url}/ucp.php?mode=logout",
                    timeout=Config.REQUEST_TIMEOUT
                )
            except requests.RequestException:
                pass

        self.session.cookies.clear()
        self.logged_in = False
        logger.info("Logged out")
