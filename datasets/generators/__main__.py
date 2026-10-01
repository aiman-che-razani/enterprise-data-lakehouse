"""
CLI entry point for the RetailNexus synthetic data generator.

Usage:
    python -m datasets.generators [options]

Options:
    --scale-factor FLOAT   Scale factor (0.01=dev, 0.1=integration, 1.0=full). Default: SCALE_FACTOR env or 1.0
    --domains DOMAIN ...   Space-separated list of domains to generate (default: all)
    --output-dir PATH      Output root directory (default: datasets/raw)
    --seed INT             Random seed for reproducibility (default: 42)
    --resume               Resume from existing registry checkpoint if present
    --no-summary           Suppress the generation summary table
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from datasets.generators.config import GeneratorConfig
from datasets.generators.registries import Registry
from src.common.logging import get_logger, setup_logging

_DOMAIN_ORDER = [
    "suppliers",
    "stores",
    "employees",
    "products",
    "customers",
    "campaigns",
    "orders",
    "order_lines",
    "payments",
    "shipments",
    "returns",
    "inventory",
    "reviews",
    "clickstream",
    "iot_sensors",
    "finance",
]

_REGISTRY_CHECKPOINT = Path("datasets/raw/.registry_checkpoint.json")


def _build_generator_map(config: GeneratorConfig, registry: Registry) -> dict:
    # Lazy imports so the CLI is fast when running --help
    from datasets.generators.domains.campaigns import CampaignsGenerator
    from datasets.generators.domains.clickstream import ClickstreamGenerator
    from datasets.generators.domains.customers import CustomersGenerator
    from datasets.generators.domains.employees import EmployeesGenerator
    from datasets.generators.domains.finance import FinanceGenerator
    from datasets.generators.domains.inventory import InventoryGenerator
    from datasets.generators.domains.iot_sensors import IotSensorsGenerator
    from datasets.generators.domains.order_lines import OrderLinesGenerator
    from datasets.generators.domains.orders import OrdersGenerator
    from datasets.generators.domains.payments import PaymentsGenerator
    from datasets.generators.domains.products import ProductsGenerator
    from datasets.generators.domains.returns import ReturnsGenerator
    from datasets.generators.domains.reviews import ReviewsGenerator
    from datasets.generators.domains.shipments import ShipmentsGenerator
    from datasets.generators.domains.stores import StoresGenerator
    from datasets.generators.domains.suppliers import SuppliersGenerator

    return {
        "suppliers": SuppliersGenerator(config, registry),
        "stores": StoresGenerator(config, registry),
        "employees": EmployeesGenerator(config, registry),
        "products": ProductsGenerator(config, registry),
        "customers": CustomersGenerator(config, registry),
        "campaigns": CampaignsGenerator(config, registry),
        "orders": OrdersGenerator(config, registry),
        "order_lines": OrderLinesGenerator(config, registry),
        "payments": PaymentsGenerator(config, registry),
        "shipments": ShipmentsGenerator(config, registry),
        "returns": ReturnsGenerator(config, registry),
        "inventory": InventoryGenerator(config, registry),
        "reviews": ReviewsGenerator(config, registry),
        "clickstream": ClickstreamGenerator(config, registry),
        "iot_sensors": IotSensorsGenerator(config, registry),
        "finance": FinanceGenerator(config, registry),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m datasets.generators",
        description="Generate synthetic RetailNexus data for all 14 operational domains.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=None,
        help="Scale factor (0.01=dev, 0.1=integration, 1.0=full). Overrides SCALE_FACTOR env var.",
    )
    parser.add_argument(
        "--domains",
        nargs="+",
        choices=_DOMAIN_ORDER,
        help="Specific domains to generate (default: all in dependency order).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output root directory (default: datasets/raw).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42).")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint registry if present.",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Suppress generation summary.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    setup_logging(service_name="datagen")
    log = get_logger(__name__)

    args = parse_args(argv)

    # Build config
    import os

    if args.scale_factor is not None:
        os.environ["SCALE_FACTOR"] = str(args.scale_factor)

    config = GeneratorConfig(seed=args.seed)
    if args.output_dir:
        config = GeneratorConfig(seed=args.seed, output_dir=args.output_dir)

    # Determine domains to run in dependency order
    requested = set(args.domains) if args.domains else set(_DOMAIN_ORDER)
    domains_to_run = [d for d in _DOMAIN_ORDER if d in requested]

    # Load or create registry
    registry: Registry
    if args.resume and _REGISTRY_CHECKPOINT.exists():
        log.info("registry_checkpoint_found", path=str(_REGISTRY_CHECKPOINT))
        registry = Registry.load(_REGISTRY_CHECKPOINT)
    else:
        registry = Registry()

    log.info(
        "generation_starting",
        scale_factor=config.scale_factor,
        seed=config.seed,
        domains=domains_to_run,
        output_dir=str(config.output_dir),
    )

    t_total = time.perf_counter()
    generators = _build_generator_map(config, registry)

    failed: list[str] = []
    for domain in domains_to_run:
        gen = generators[domain]
        try:
            gen.generate()
            _REGISTRY_CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
            registry.save(_REGISTRY_CHECKPOINT)
            log.info("checkpoint_saved", domain=domain)
        except Exception as exc:
            log.error("generator_failed", domain=domain, error=str(exc), exc_info=True)
            failed.append(domain)

    elapsed = time.perf_counter() - t_total

    if not args.no_summary:
        # The summary uses box-drawing characters; a cp1252 Windows console cannot encode them.
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        print(registry.summary())
        print(f"  Total wall-clock time: {elapsed:.1f}s\n")

    if failed:
        log.error("some_generators_failed", failed=failed)
        return 1

    log.info("generation_complete", elapsed_s=round(elapsed, 2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
