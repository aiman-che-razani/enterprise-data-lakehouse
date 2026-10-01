"""
Clickstream event generator — 5,000,000 events across web and mobile sessions.

Events are generated as sessions (3–20 events each) to preserve realistic
funnel progression: page_view → search → product_view → add_to_cart → checkout → purchase.
Output is newline-delimited JSON, gzip-compressed.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator
from src.common.logging import get_logger

_DEVICE_TYPES = {"desktop": 0.45, "mobile": 0.40, "tablet": 0.15}

_BROWSERS = {
    "Chrome": 0.62,
    "Safari": 0.20,
    "Firefox": 0.08,
    "Edge": 0.07,
    "Samsung Internet": 0.03,
}

_OS_BY_DEVICE = {
    "desktop": ["Windows", "macOS", "Linux"],
    "mobile": ["iOS", "Android"],
    "tablet": ["iOS", "Android", "Windows"],
}

_EVENT_TYPES = [
    "page_view",
    "search",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "wishlist_add",
    "checkout_start",
    "checkout_step",
    "purchase",
    "account_login",
    "account_logout",
]

# Funnel: session starts with page_view/search, possibly ends in purchase
_SESSION_FUNNEL = {
    "browse": ["page_view", "search", "product_view"],
    "add_to_cart": ["page_view", "search", "product_view", "add_to_cart"],
    "abandoned_cart": ["page_view", "product_view", "add_to_cart", "checkout_start"],
    "purchase": [
        "page_view",
        "search",
        "product_view",
        "add_to_cart",
        "checkout_start",
        "checkout_step",
        "purchase",
    ],
}

_FUNNEL_WEIGHTS = {"browse": 0.55, "add_to_cart": 0.20, "abandoned_cart": 0.15, "purchase": 0.10}

_PAGE_CATEGORIES = [
    "/home",
    "/category/{cat}",
    "/product/{sku}",
    "/search?q={term}",
    "/cart",
    "/checkout/shipping",
    "/checkout/payment",
    "/order/confirmation",
    "/account/orders",
    "/account/profile",
    "/returns",
    "/deals",
]

_SEARCH_TERMS = [
    "headphones",
    "running shoes",
    "laptop stand",
    "kitchen appliances",
    "winter jacket",
    "coffee maker",
    "yoga mat",
    "air fryer",
    "gaming chair",
    "desk lamp",
    "phone case",
    "skincare",
]

_UTM_SOURCES = {
    "google": 0.30,
    "facebook": 0.15,
    "email": 0.20,
    "direct": 0.20,
    "instagram": 0.08,
    "tiktok": 0.04,
    "affiliate": 0.03,
}


class ClickstreamGenerator(BaseGenerator):
    domain = "clickstream"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.clickstream

    def generate(self) -> list[dict[str, Any]]:
        log = get_logger(__name__)
        t0 = time.perf_counter()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        target = self.config.counts.clickstream
        all_records: list[dict[str, Any]] = []
        event_count = 0
        session_idx = 1

        log.info("generator_started", domain=self.domain, target_events=target)

        while event_count < target:
            session_events = self._generate_session(session_idx)
            all_records.extend(session_events)
            event_count += len(session_events)
            session_idx += 1

            if event_count % 250_000 == 0:
                log.info(
                    "generator_progress",
                    domain=self.domain,
                    events=event_count,
                    sessions=session_idx,
                )

        # Trim to target
        all_records = all_records[:target]

        # Write as gzip JSON (chunked)
        chunk_size = self.config.chunk_size
        for chunk_idx, start in enumerate(range(0, len(all_records), chunk_size)):
            import gzip
            import json

            chunk = all_records[start : start + chunk_size]
            path = self.output_dir / f"{self.domain}_part_{chunk_idx:04d}.json.gz"
            content = "\n".join(json.dumps(r) for r in chunk)
            with gzip.open(path, "wt", encoding="utf-8") as f:
                f.write(content)

        duration = time.perf_counter() - t0
        self.registry.record_stat(self.domain, len(all_records), duration)
        log.info(
            "generator_complete",
            domain=self.domain,
            count=len(all_records),
            sessions=session_idx,
            duration_s=round(duration, 2),
        )
        return all_records

    def _generate_session(self, session_idx: int) -> list[dict[str, Any]]:
        session_id = f"SES-{session_idx:012d}"

        # ~70% of sessions are from known customers
        customer_id: str | None = None
        if self.registry.customer_ids and self.rng.random() < 0.70:
            customer_id = self._choose(self.registry.customer_ids)

        anonymous_id = f"ANON-{self.fake.uuid4()}"
        device_type = self._weighted_choice(_DEVICE_TYPES)
        browser = self._weighted_choice(_BROWSERS)
        os_name = self._choose(_OS_BY_DEVICE[device_type])
        ip = self.fake.ipv4_public()
        utm_source = self._weighted_choice(_UTM_SOURCES) if self.rng.random() > 0.3 else None

        # Session start time
        session_start = self._random_datetime()

        funnel_type = self._weighted_choice(_FUNNEL_WEIGHTS)
        funnel_events = _SESSION_FUNNEL[funnel_type]

        # Add some noise events between funnel steps
        all_events = []
        current_dt = session_start
        referrer = f"https://{utm_source}.com" if utm_source else None

        for i, event_type in enumerate(funnel_events):
            sku: str | None = None
            campaign_id: str | None = None
            page_url = self._pick_page_url(event_type)

            if "product" in event_type or event_type in ("add_to_cart",):
                skus = self.registry.active_skus or self.registry.product_skus
                sku = self._choose(skus) if skus else None

            if self.registry.campaign_ids and self.rng.random() < 0.20:
                campaign_id = self._choose(self.registry.campaign_ids)

            duration_ms = int(self.rng.integers(1_000, 120_000))
            current_dt += timedelta(milliseconds=duration_ms + int(self.rng.integers(0, 5_000)))

            all_events.append(
                {
                    "event_id": f"EVT-{self.fake.uuid4()}",
                    "session_id": session_id,
                    "customer_id": customer_id,
                    "anonymous_id": anonymous_id,
                    "event_type": event_type,
                    "page_url": page_url,
                    "referrer_url": referrer if i == 0 else None,
                    "sku": sku,
                    "campaign_id": campaign_id,
                    "utm_source": utm_source,
                    "utm_medium": "cpc" if utm_source in ("google", "facebook") else None,
                    "device_type": device_type,
                    "browser": browser,
                    "os": os_name,
                    "ip_address": ip,
                    "session_duration_ms": duration_ms,
                    "is_bounce": len(funnel_events) == 1,
                    "search_query": (
                        self._choose(_SEARCH_TERMS) if event_type == "search" else None
                    ),
                    "created_at": self._iso_dt(current_dt),
                }
            )
            referrer = page_url

        return all_events

    def _pick_page_url(self, event_type: str) -> str:
        base = "https://retailnexus.com"
        if event_type == "page_view":
            return f"{base}{self._choose(['/home', '/deals', '/new-arrivals', '/brands'])}"
        if event_type == "search":
            return f"{base}/search?q={self._choose(_SEARCH_TERMS).replace(' ', '+')}"
        if event_type == "product_view":
            skus = self.registry.active_skus or ["SKU-00000001"]
            return f"{base}/product/{self._choose(skus)}"
        if event_type == "add_to_cart":
            return f"{base}/cart"
        if event_type == "checkout_start":
            return f"{base}/checkout"
        if event_type == "checkout_step":
            return f"{base}/checkout/shipping"
        if event_type == "purchase":
            return f"{base}/order/confirmation"
        return f"{base}{self._choose(_PAGE_CATEGORIES).split('{')[0]}"

    def _generate_record(self, index: int) -> dict[str, Any]:
        raise NotImplementedError("ClickstreamGenerator overrides generate() directly")
