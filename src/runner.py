import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

# Make the src folder importable when running from repo root
CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(CURRENT_DIR) not in os.sys.path:
    os.sys.path.insert(0, str(CURRENT_DIR))

# Imports from src/*
from extractors.medium_parser import fetch_profile, normalize_profile_url
from extractors.utils_filter import (
    apply_keyword_filter,
    apply_domain_filter,
    apply_location_filter,
    dedupe_by_url,
)
from outputs.exporters import export_records
from config_loader import load_settings

logger = logging.getLogger("medium_scraper")

def load_inputs(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Input JSON must be an object with keys like 'profiles'.")
    data.setdefault("profiles", [])
    data.setdefault("keywords", [])
    data.setdefault("email_domains", [])
    data.setdefault("location_contains", [])
    data.setdefault("max_items", 100)
    data.setdefault("output", {})
    data["output"].setdefault("format", "json")
    data["output"].setdefault("path", str(ROOT_DIR / "data" / "sample_output.json"))
    return data

def process_profiles(
    urls: List[str],
    max_items: int,
    concurrency: int,
    request_timeout: float,
    user_agent: str,
) -> List[Dict[str, Any]]:
    """
    Sequential by default (robust and Medium-friendly). Concurrency kept as a parameter
    for potential expansion; we keep fetches friendly to avoid rate limits.
    """
    results: List[Dict[str, Any]] = []
    for i, url in enumerate(urls):
        if len(results) >= max_items:
            break
        try:
            normalized = normalize_profile_url(url)
            profile = fetch_profile(
                normalized,
                timeout=request_timeout,
                user_agent=user_agent,
            )
            if profile:
                results.append(profile)
        except Exception as e:
            logger.exception("Failed to process %s: %s", url, e)
    return results

def main():
    parser = argparse.ArgumentParser(
        description="Medium Email Scraper – Advanced, Cheapest & Reliable"
    )
    parser.add_argument(
        "-i",
        "--input",
        default=str(ROOT_DIR / "data" / "inputs.sample.json"),
        help="Path to input JSON file describing profiles and filters.",
    )
    parser.add_argument(
        "-s",
        "--settings",
        default=str(CURRENT_DIR / "config" / "settings.example.json"),
        help="Path to settings JSON (timeouts, headers, defaults).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Override output path (format inferred from extension).",
    )
    args = parser.parse_args()

    settings = load_settings(Path(args.settings))
    log_level = settings.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    inputs = load_inputs(Path(args.input))
    if args.output:
        # Inferred format from extension if not specified explicitly
        inputs["output"]["path"] = args.output
        ext = Path(args.output).suffix.lower().lstrip(".")
        if ext in {"json", "csv", "xlsx", "xml"}:
            inputs["output"]["format"] = "excel" if ext == "xlsx" else ext

    profiles: List[str] = [normalize_profile_url(u) for u in inputs["profiles"]]
    if not profiles:
        logger.warning(
            "No profiles supplied in inputs. Provide Medium profile URLs under 'profiles'."
        )

    logger.info("Starting scrape for %d profiles", len(profiles))

    raw_records = process_profiles(
        urls=profiles,
        max_items=int(inputs.get("max_items", 100)),
        concurrency=int(settings.get("CONCURRENCY", 2)),
        request_timeout=float(settings.get("REQUEST_TIMEOUT", 15.0)),
        user_agent=str(settings.get("USER_AGENT")),
    )

    # Dedupe before filtering
    records = dedupe_by_url(raw_records)

    # Filters
    if inputs.get("keywords"):
        records = apply_keyword_filter(records, inputs["keywords"])

    if inputs.get("email_domains"):
        records = apply_domain_filter(records, inputs["email_domains"])

    if inputs.get("location_contains"):
        records = apply_location_filter(records, inputs["location_contains"])

    # Respect max_items after filters too
    max_items = int(inputs.get("max_items", 100))
    if len(records) > max_items:
        records = records[:max_items]

    out_fmt = inputs["output"]["format"]
    out_path = Path(inputs["output"]["path"]).resolve()

    logger.info("Exporting %d records to %s (%s)", len(records), out_path, out_fmt)
    export_records(records, out_path, out_fmt)
    logger.info("Done.")

if __name__ == "__main__":
    main()