"""Marketing campaign generator — 10,000 campaigns (full scale)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

_CAMPAIGN_TYPES = {
    "email": 0.30,
    "paid_search": 0.20,
    "social_media": 0.18,
    "display": 0.12,
    "affiliate": 0.08,
    "sms": 0.07,
    "push_notification": 0.05,
}

_OBJECTIVES = {
    "awareness": 0.25,
    "acquisition": 0.30,
    "retention": 0.25,
    "win_back": 0.10,
    "upsell": 0.10,
}

_CHANNELS_BY_TYPE = {
    "email": ["Mailchimp", "Salesforce Marketing Cloud", "Klaviyo", "HubSpot"],
    "paid_search": ["Google Ads", "Microsoft Ads"],
    "social_media": ["Facebook Ads", "Instagram Ads", "TikTok Ads", "Pinterest Ads"],
    "display": ["Google Display Network", "The Trade Desk", "Amazon DSP"],
    "affiliate": ["CJ Affiliate", "ShareASale", "Impact"],
    "sms": ["Attentive", "Klaviyo SMS", "Twilio"],
    "push_notification": ["OneSignal", "Braze", "Iterable"],
}

_PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing & Apparel",
    "Home & Garden",
    "Sports & Outdoors",
    "Beauty & Personal Care",
    "Food & Beverage",
    "Toys & Games",
    "All Categories",
]


class CampaignsGenerator(BaseGenerator):
    domain = "campaigns"
    output_format = "csv"

    def record_count(self) -> int:
        return self.config.counts.campaigns

    def _generate_record(self, index: int) -> dict[str, Any]:
        campaign_id = f"CAM-{index + 1:07d}"
        campaign_type = self._weighted_choice(_CAMPAIGN_TYPES)
        objective = self._weighted_choice(_OBJECTIVES)
        platform = self._choose(_CHANNELS_BY_TYPE[campaign_type])

        start_dt = self._random_datetime(end=self.config.end_date)
        duration_days = int(self.rng.choice([7, 14, 21, 28, 30, 60, 90]))
        end_dt = start_dt + timedelta(days=duration_days)

        # Budget scales with type and platform
        base_budget = {
            "email": 500,
            "paid_search": 5_000,
            "social_media": 3_000,
            "display": 2_000,
            "affiliate": 1_000,
            "sms": 800,
            "push_notification": 200,
        }[campaign_type]
        budget = round(float(self.rng.uniform(base_budget * 0.5, base_budget * 5.0)), 2)

        # Spend = some fraction of budget (campaigns in progress don't spend all)
        spend_pct = float(self.rng.uniform(0.6, 1.05))
        actual_spend = round(min(budget * spend_pct, budget), 2)

        # Performance metrics (correlated with budget and duration)
        impressions = int(actual_spend * self.rng.uniform(50, 500))
        ctr = float(self.rng.uniform(0.005, 0.08))
        clicks = int(impressions * ctr)
        cvr = float(self.rng.uniform(0.01, 0.12))
        conversions = int(clicks * cvr)
        revenue_per_conversion = float(self.rng.uniform(20, 200))
        total_revenue = round(conversions * revenue_per_conversion, 2)
        roas = round(total_revenue / max(actual_spend, 1), 3)

        is_active = end_dt.date() >= self.config.end_date and bool(self.rng.random() > 0.3)

        record: dict[str, Any] = {
            "campaign_id": campaign_id,
            "campaign_name": f"{objective.replace('_', ' ').title()} — {platform} — {self._iso_date(start_dt.date())}",
            "campaign_type": campaign_type,
            "objective": objective,
            "platform": platform,
            "target_category": self._choose(_PRODUCT_CATEGORIES),
            "target_segment": self._weighted_choice(
                {
                    "all": 0.40,
                    "platinum": 0.15,
                    "gold": 0.20,
                    "silver": 0.15,
                    "bronze": 0.10,
                }
            ),
            "budget": budget,
            "actual_spend": actual_spend,
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(ctr, 5),
            "conversions": conversions,
            "conversion_rate": round(cvr, 5),
            "total_revenue": total_revenue,
            "roas": roas,
            "cpc": round(actual_spend / max(clicks, 1), 4),
            "cpa": round(actual_spend / max(conversions, 1), 2),
            "is_active": is_active,
            "start_date": self._iso_dt(start_dt),
            "end_date": self._iso_dt(end_dt),
            "created_at": self._iso_dt(start_dt),
            "updated_at": self._iso_dt(
                self._random_datetime(start=start_dt.date(), end=self.config.end_date)
            ),
        }

        self.registry.campaign_ids.append(campaign_id)
        self.registry.campaign_types[campaign_id] = campaign_type

        return record
