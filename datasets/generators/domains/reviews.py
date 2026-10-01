"""Product reviews generator — 200,000 records (full scale)."""

from __future__ import annotations

from datetime import UTC, timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

# Rating distribution: skewed positive (J-shaped)
_RATING_WEIGHTS = {5: 0.52, 4: 0.22, 3: 0.12, 2: 0.07, 1: 0.07}

# Sentiment from rating (with some noise)
_SENTIMENT_BY_RATING = {
    5: "positive",
    4: "positive",
    3: "neutral",
    2: "negative",
    1: "negative",
}

_REVIEW_TITLES_BY_RATING = {
    5: [
        "Absolutely love it!",
        "Exceeded expectations",
        "Best purchase ever",
        "Highly recommend",
        "Perfect product",
        "Five stars!",
        "Amazing quality",
    ],
    4: [
        "Great product overall",
        "Very happy with this",
        "Good value for money",
        "Would recommend",
        "Solid purchase",
        "Mostly great",
        "Does what it says",
    ],
    3: [
        "It's okay",
        "Does the job",
        "Nothing special",
        "Average product",
        "Met basic expectations",
        "Some pros and cons",
        "Could be better",
    ],
    2: [
        "Disappointed",
        "Not as expected",
        "Below average quality",
        "Had issues with this",
        "Wouldn't buy again",
        "Had potential but...",
    ],
    1: [
        "Terrible product",
        "Waste of money",
        "Do not buy",
        "Very disappointed",
        "Defective product",
        "Horrible experience",
    ],
}

_SENTIMENT_PHRASES = {
    "positive": [
        "works exactly as described",
        "arrived quickly",
        "great build quality",
        "well worth the price",
        "packaging was perfect",
        "easy to set up",
    ],
    "negative": [
        "stopped working after a week",
        "not as pictured",
        "cheaply made",
        "difficult to assemble",
        "missing parts",
        "overpriced for the quality",
    ],
    "neutral": [
        "nothing unexpected",
        "does what it says",
        "average quality",
        "okay for the price",
        "took a while to arrive",
    ],
}


class ReviewsGenerator(BaseGenerator):
    domain = "reviews"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.reviews

    def _generate_record(self, index: int) -> dict[str, Any]:
        review_id = f"REV-{index + 1:010d}"

        customer_id = (
            self._choose(self.registry.customer_ids)
            if self.registry.customer_ids
            else f"CUST-{int(self.rng.integers(1, 500000)):08d}"
        )
        skus = self.registry.active_skus or self.registry.product_skus
        sku = self._choose(skus) if skus else f"SKU-{int(self.rng.integers(1, 50000)):08d}"

        # Associate with a real order if possible
        order_id: str | None = None
        if self.registry.order_ids and self.rng.random() > 0.05:
            order_id = self._choose(self.registry.order_ids)

        rating = int(self._weighted_choice({str(k): v for k, v in _RATING_WEIGHTS.items()}))
        sentiment = _SENTIMENT_BY_RATING[rating]

        title = self._choose(_REVIEW_TITLES_BY_RATING[rating])
        phrase = self._choose(_SENTIMENT_PHRASES[sentiment])
        product_name = self.registry.product_names.get(sku, "this product")
        body = (
            f"I purchased {product_name} and {phrase}. "
            f"{self.fake.sentence(nb_words=12)} "
            f"{self.fake.sentence(nb_words=8)}"
        )

        # Review written 7-90 days after order
        review_dt = self._random_datetime()
        if order_id:
            order_dt_str = self.registry.order_dates.get(order_id)
            if order_dt_str:
                from datetime import datetime

                order_dt = datetime.strptime(order_dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
                delay_days = int(self.rng.integers(7, 91))
                review_dt = order_dt + timedelta(days=delay_days)
                if review_dt.year > self.config.end_date.year:
                    review_dt = order_dt + timedelta(days=30)

        helpful_votes = int(self.rng.integers(0, 50))
        not_helpful_votes = int(self.rng.integers(0, max(1, helpful_votes // 3)))

        return {
            "review_id": review_id,
            "customer_id": customer_id,
            "sku": sku,
            "product_name": self.registry.product_names.get(sku, ""),
            "order_id": order_id,
            "rating": rating,
            "title": title,
            "body": body,
            "sentiment": sentiment,
            "is_verified_purchase": bool(order_id is not None and self.rng.random() > 0.1),
            "helpful_votes": helpful_votes,
            "not_helpful_votes": not_helpful_votes,
            "is_visible": bool(self.rng.random() > 0.04),
            "response_from_seller": (
                self.fake.sentence(nb_words=15) if rating <= 2 and self.rng.random() > 0.6 else None
            ),
            "images_count": int(
                self.rng.choice([0, 0, 0, 1, 2, 3], p=[0.70, 0.10, 0.08, 0.07, 0.03, 0.02])
            ),
            "created_at": self._iso_dt(review_dt),
            "updated_at": self._iso_dt(review_dt + timedelta(days=int(self.rng.integers(0, 30)))),
        }
