import html
import logging
import re
from dataclasses import dataclass, asdict
from typing import Dict, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger("medium_scraper.parser")

EMAIL_RE = re.compile(
    r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9\-.]+)", re.IGNORECASE
)

DESCRIPTION_META_NAMES = [
    "description",
    "og:description",
    "twitter:description",
]

@dataclass
class ProfileRecord:
    title: str
    url: str
    snippet: str
    email: Optional[str]
    email_domain: Optional[str]
    platform: str = "medium"
    displayed_url: Optional[str] = None
    # Optional fields discovered heuristically
    location: Optional[str] = None

def normalize_profile_url(url: str) -> str:
    """
    Normalize Medium profile URLs into canonical https://medium.com/@handle form or publication user pages.
    Accepts: https://medium.com/@handle , https://medium.com/u/<id>, or https://<pub>.medium.com/@handle
    """
    if not url:
        raise ValueError("Empty URL")
    url = url.strip()
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or "medium.com"
    path = parsed.path or "/"

    # Strip tracking params, fragments
    normalized = f"{scheme}://{netloc}{path}"
    normalized = normalized.rstrip("/")
    return normalized

def _displayed_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path.lstrip("/")
    return f"{host}/{path}" if path else host

class RequestError(Exception):
    pass

@retry(
    wait=wait_exponential(multiplier=1, min=1, max=8),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(RequestError),
    reraise=True,
)
def _get(url: str, timeout: float, user_agent: str) -> requests.Response:
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        raise RequestError(str(e)) from e

    if resp.status_code >= 500:
        # transient
        raise RequestError(f"Server error {resp.status_code}")
    return resp

def _extract_title(soup: BeautifulSoup) -> str:
    # Prefer explicit tags
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        return html.unescape(og_title["content"]).strip()

    tw_title = soup.find("meta", attrs={"name": "twitter:title"})
    if tw_title and tw_title.get("content"):
        return html.unescape(tw_title["content"]).strip()

    if soup.title and soup.title.text:
        return html.unescape(soup.title.text).strip()

    # Fallback
    return "Untitled"

def _extract_description(soup: BeautifulSoup) -> str:
    for name in DESCRIPTION_META_NAMES:
        node = soup.find("meta", attrs={"name": name}) or soup.find(
            "meta", attrs={"property": name}
        )
        if node and node.get("content"):
            return html.unescape(node["content"]).strip()

    # Heuristic: Medium profile bio may live in <meta name="description"> or inside article header
    # Try to find a likely bio block
    candidates = soup.select("p, h2, h3")
    for el in candidates[:40]:
        txt = el.get_text(separator=" ", strip=True)
        if txt and 20 <= len(txt) <= 280:
            return txt
    return ""

def _extract_email(text: str) -> Optional[str]:
    # Prefer direct occurrences; ignore obvious obfuscations for now
    matches = EMAIL_RE.findall(text or "")
    if not matches:
        return None
    # Choose the first plausible one
    for addr in matches:
        # Filter out Medium internal email placeholders if any
        if ".medium." in addr:
            continue
        return addr
    return None

def _extract_location_heuristic(soup: BeautifulSoup) -> Optional[str]:
    """
    Very light heuristic to catch common 'Location' hints on a profile page.
    """
    text = soup.get_text(separator=" ", strip=True)
    # Look for "Based in <City/Country>" or "Location: <...>"
    m = re.search(r"(Based in|Location[: ]+)([^|·\n\r]{2,50})", text, re.IGNORECASE)
    if m:
        location = m.group(2).strip(" :·|-")
        if 2 <= len(location) <= 50:
            return location
    return None

def fetch_profile(url: str, timeout: float = 15.0, user_agent: str = "Mozilla/5.0") -> Optional[Dict]:
    """
    Fetch and parse a Medium profile URL into a structured record.
    Returns None if the page doesn't look like a valid profile.
    """
    logger.debug("Fetching %s", url)
    resp = _get(url, timeout=timeout, user_agent=user_agent)

    # Basic content-type sanity
    ctype = resp.headers.get("Content-Type", "")
    if "html" not in ctype:
        logger.warning("Non-HTML content for %s: %s", url, ctype)
        return None

    html_text = resp.text
    soup = BeautifulSoup(html_text, "lxml")

    title = _extract_title(soup)
    snippet = _extract_description(soup)
    # Search email in combined text (meta + body)
    text_for_email = " ".join(
        filter(None, [title, snippet, soup.get_text(separator=" ", strip=True)])
    )
    email = _extract_email(text_for_email)
    email_domain = email.split("@", 1)[1].lower() if email else None
    location = _extract_location_heuristic(soup)

    record = ProfileRecord(
        title=title,
        url=url,
        snippet=snippet,
        email=email,
        email_domain=email_domain,
        displayed_url=_displayed_url(url),
        location=location,
    )
    return asdict(record)