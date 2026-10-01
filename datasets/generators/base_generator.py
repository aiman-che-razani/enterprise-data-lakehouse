"""
Abstract base class for all RetailNexus data generators.

Subclasses implement `domain`, `output_format`, and `_generate_record()`.
The base handles: chunked file writing, progress reporting, stats recording,
CSV/JSON/Parquet serialisation, and seeded randomness.
"""

from __future__ import annotations

import csv
import gzip
import json
import time
import uuid
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

import numpy as np
from faker import Faker

from src.common.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

    from datasets.generators.config import GeneratorConfig
    from datasets.generators.registries import Registry

logger = get_logger(__name__)


class BaseGenerator(ABC):
    """
    Abstract base for all domain data generators.

    Usage:
        class CustomersGenerator(BaseGenerator):
            domain = "customers"
            output_format = "json"

            def record_count(self) -> int:
                return self.config.counts.customers

            def _generate_record(self, index: int) -> dict[str, Any]:
                ...
    """

    domain: str
    output_format: Literal["json", "csv", "parquet"]

    def __init__(self, config: GeneratorConfig, registry: Registry) -> None:
        self.config = config
        self.registry = registry

        # Seeded Faker — deterministic names/addresses
        self.fake = Faker(["en_US"])
        Faker.seed(config.seed)

        # Seeded NumPy RNG — deterministic distributions
        self.rng = np.random.default_rng(config.seed + hash(self.__class__.__name__) % 2**31)

        # Output directory for this domain
        self.output_dir = config.output_dir / self.output_format / self.domain

    # ─── Abstract interface ───────────────────────────────────────────────────

    @abstractmethod
    def record_count(self) -> int:
        """Total number of records to generate."""

    @abstractmethod
    def _generate_record(self, index: int) -> dict[str, Any]:
        """
        Generate a single record for the given sequential index.
        Must be deterministic given the same index and seed.
        """

    # ─── Public generate method ───────────────────────────────────────────────

    def generate(self) -> list[dict[str, Any]]:
        """
        Generate all records and write them to disk.

        Returns the list of all generated records (for use by other generators
        that need to populate the Registry from the output).
        """
        count = self.record_count()
        logger.info("generator_started", domain=self.domain, count=count, format=self.output_format)
        t0 = time.perf_counter()

        self.output_dir.mkdir(parents=True, exist_ok=True)

        records = list(self._generate_all(count))

        if self.output_format == "json":
            self._write_json(records)
        elif self.output_format == "csv":
            self._write_csv(records)
        elif self.output_format == "parquet":
            self._write_parquet(records)

        duration = time.perf_counter() - t0
        self.registry.record_stat(self.domain, count, duration)
        logger.info(
            "generator_complete",
            domain=self.domain,
            count=count,
            duration_s=round(duration, 2),
            rps=round(count / max(duration, 0.001)),
        )
        return records

    # ─── Private helpers ──────────────────────────────────────────────────────

    def _generate_all(self, count: int) -> Iterator[dict[str, Any]]:
        chunk_size = self.config.chunk_size
        for i in range(count):
            if i > 0 and i % chunk_size == 0:
                logger.info(
                    "generator_progress",
                    domain=self.domain,
                    progress=f"{i:,}/{count:,}",
                    pct=round(100 * i / count, 1),
                )
            yield self._generate_record(i)

    def _write_json(self, records: list[dict[str, Any]]) -> None:
        chunk_size = self.config.chunk_size
        compress = self.config.compress_json

        for chunk_idx, start in enumerate(range(0, len(records), chunk_size)):
            chunk = records[start : start + chunk_size]
            suffix = ".json.gz" if compress else ".json"
            path = self.output_dir / f"{self.domain}_part_{chunk_idx:04d}{suffix}"

            content = "\n".join(json.dumps(r, default=_json_default) for r in chunk)
            if compress:
                with gzip.open(path, "wt", encoding="utf-8") as f:
                    f.write(content)
            else:
                path.write_text(content, encoding="utf-8")

        logger.debug("json_written", domain=self.domain, chunks=chunk_idx + 1)  # type: ignore[possibly-undefined]

    def _write_csv(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        path = self.output_dir / f"{self.domain}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)
        logger.debug("csv_written", domain=self.domain, path=str(path))

    def _write_parquet(self, records: list[dict[str, Any]]) -> None:
        try:
            import pandas as pd

            df = pd.DataFrame(records)
            path = self.output_dir / f"{self.domain}.parquet"
            df.to_parquet(path, index=False, compression="snappy")
            logger.debug("parquet_written", domain=self.domain, path=str(path))
        except ImportError:
            logger.warning("pandas_not_installed_falling_back_to_json", domain=self.domain)
            self.output_format = "json"  # type: ignore[misc]
            self._write_json(records)

    # ─── Random helpers shared by subclasses ─────────────────────────────────

    def _new_id(self) -> str:
        """Generate a UUID4 string."""
        return str(uuid.uuid4())

    def _random_date(
        self,
        start: date | None = None,
        end: date | None = None,
    ) -> date:
        s = start or self.config.start_date
        e = end or self.config.end_date
        delta = (e - s).days
        offset = int(self.rng.integers(0, delta + 1))
        return s + timedelta(days=offset)

    def _random_datetime(
        self,
        start: date | None = None,
        end: date | None = None,
    ) -> datetime:
        d = self._random_date(start, end)
        hour = int(self.rng.integers(0, 24))
        minute = int(self.rng.integers(0, 60))
        second = int(self.rng.integers(0, 60))
        return datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=UTC)

    def _weighted_choice(self, weights: dict[str, float]) -> str:
        """Pick a key from a dict whose values are probability weights."""
        keys = list(weights.keys())
        probs = np.array(list(weights.values()), dtype=float)
        probs /= probs.sum()
        idx = int(self.rng.choice(len(keys), p=probs))
        return keys[idx]

    def _choose(self, population: list[Any]) -> Any:
        """Choose uniformly from a list."""
        return population[int(self.rng.integers(0, len(population)))]

    def _choose_n(self, population: list[Any], n: int) -> list[Any]:
        """Choose n items without replacement from a list."""
        indices = self.rng.choice(len(population), size=min(n, len(population)), replace=False)
        return [population[i] for i in indices]

    def _iso_dt(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _iso_date(self, d: date) -> str:
        return d.strftime("%Y-%m-%d")

    def _round2(self, v: float) -> float:
        return round(float(v), 2)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    raise TypeError(f"Not JSON serializable: {type(obj)}")
