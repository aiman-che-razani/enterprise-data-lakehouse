"""
Financial general ledger generator — 500,000 GL transactions (full scale).

Generates double-entry bookkeeping transactions spanning:
  - Order revenue / cost of goods sold
  - Payroll expenses
  - Rent, utilities, SaaS subscriptions
  - Marketing spend
  - Shipping costs
  - Returns and refunds
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

_GL_ACCOUNTS = {
    # Revenue
    "4000": ("Net Sales Revenue", "revenue"),
    "4010": ("Online Channel Revenue", "revenue"),
    "4020": ("Store Channel Revenue", "revenue"),
    "4030": ("Mobile App Revenue", "revenue"),
    "4100": ("Returns and Allowances", "revenue"),
    # COGS
    "5000": ("Cost of Goods Sold", "cogs"),
    "5010": ("Inbound Freight", "cogs"),
    "5020": ("Packaging Materials", "cogs"),
    "5030": ("Warehouse Labor", "cogs"),
    # Operating Expenses
    "6000": ("Payroll — Salaries", "opex"),
    "6010": ("Payroll — Benefits", "opex"),
    "6020": ("Payroll — Contractors", "opex"),
    "6100": ("Rent and Leases", "opex"),
    "6110": ("Utilities", "opex"),
    "6120": ("Office Supplies", "opex"),
    "6200": ("Marketing and Advertising", "opex"),
    "6210": ("Digital Advertising", "opex"),
    "6220": ("Affiliate Commissions", "opex"),
    "6300": ("Software and SaaS", "opex"),
    "6310": ("Cloud Infrastructure", "opex"),
    "6400": ("Outbound Shipping", "opex"),
    "6410": ("Returns Shipping", "opex"),
    "6500": ("Depreciation", "opex"),
    # Assets
    "1000": ("Cash and Equivalents", "asset"),
    "1100": ("Accounts Receivable", "asset"),
    "1200": ("Inventory", "asset"),
    "1300": ("Prepaid Expenses", "asset"),
    # Liabilities
    "2000": ("Accounts Payable", "liability"),
    "2100": ("Accrued Liabilities", "liability"),
    "2200": ("Deferred Revenue", "liability"),
    "2300": ("Sales Tax Payable", "liability"),
}

_ACCOUNT_CODES = list(_GL_ACCOUNTS.keys())

_COST_CENTERS = [
    "CC-ECOM",
    "CC-RETAIL",
    "CC-SUPPLY",
    "CC-MARKETING",
    "CC-TECH",
    "CC-HR",
    "CC-FINANCE",
    "CC-LOGISTICS",
]

_SOURCE_SYSTEMS = ["ERP", "ORDER_MGMT", "PAYROLL", "LEASE_MGMT", "MANUAL_ENTRY"]

# Recurring monthly expenses
_RECURRING_MONTHLY = [
    ("6100", "6000", "Payroll — Salaries Monthly", 450_000, 550_000),
    ("6100", "6010", "Payroll — Benefits Monthly", 60_000, 90_000),
    ("6100", "6100", "Rent — HQ", 45_000, 55_000),
    ("6100", "6110", "Utilities — All Locations", 15_000, 25_000),
    ("6100", "6300", "SaaS Subscriptions", 40_000, 60_000),
    ("6100", "6310", "AWS Cloud", 25_000, 45_000),
]


class FinanceGenerator(BaseGenerator):
    domain = "finance"
    output_format = "parquet"

    def record_count(self) -> int:
        return self.config.counts.finance

    def _generate_record(self, index: int) -> dict[str, Any]:
        transaction_id = f"TXN-{index + 1:010d}"

        # Transaction type determines account pair and amounts
        txn_type = self._weighted_choice(
            {
                "revenue": 0.30,
                "cogs": 0.20,
                "payroll": 0.12,
                "marketing": 0.10,
                "logistics": 0.10,
                "rent_utilities": 0.08,
                "refunds": 0.05,
                "cloud_saas": 0.05,
            }
        )

        debit_account, credit_account, description, amount = self._txn_by_type(txn_type)
        txn_date = self._random_date()
        fiscal_year = txn_date.year
        fiscal_period = txn_date.month

        # Associate with order if it's revenue/cogs
        reference_id: str | None = None
        if txn_type in ("revenue", "cogs") and self.registry.order_ids:
            reference_id = self._choose(self.registry.order_ids)

        debit_name, debit_cat = _GL_ACCOUNTS.get(debit_account, ("Unknown", "unknown"))
        credit_name, credit_cat = _GL_ACCOUNTS.get(credit_account, ("Unknown", "unknown"))

        return {
            "transaction_id": transaction_id,
            "journal_entry_id": f"JE-{(index // 2) + 1:010d}",
            "transaction_type": txn_type,
            "debit_account_code": debit_account,
            "debit_account_name": debit_name,
            "credit_account_code": credit_account,
            "credit_account_name": credit_name,
            "debit_amount": amount,
            "credit_amount": amount,
            "description": description,
            "reference_id": reference_id,
            "cost_center": self._choose(_COST_CENTERS),
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "fiscal_quarter": (fiscal_period - 1) // 3 + 1,
            "transaction_date": self._iso_date(txn_date),
            "posted_at": self._iso_dt(
                self._random_datetime(start=txn_date, end=txn_date + timedelta(days=3))
            ),
            "source_system": self._choose(_SOURCE_SYSTEMS),
            "is_reconciled": bool(self.rng.random() > 0.05),
            "is_reversed": bool(self.rng.random() < 0.02),
            "created_at": self._iso_dt(self._random_datetime(end=txn_date)),
        }

    def _txn_by_type(self, txn_type: str) -> tuple[str, str, str, float]:
        """Return (debit_account, credit_account, description, amount)."""
        r = self.rng
        if txn_type == "revenue":
            revenue_sub = self._weighted_choice({"4010": 0.45, "4020": 0.30, "4030": 0.25})
            amount = round(float(r.uniform(15.0, 2500.0)), 2)
            return "1000", revenue_sub, "Customer order payment received", amount

        if txn_type == "cogs":
            amount = round(float(r.uniform(8.0, 1200.0)), 2)
            return "5000", "1200", "Cost of goods sold — order fulfillment", amount

        if txn_type == "payroll":
            account = self._weighted_choice({"6000": 0.70, "6010": 0.20, "6020": 0.10})
            name, _ = _GL_ACCOUNTS[account]
            amount = round(float(r.uniform(2_000.0, 25_000.0)), 2)
            return account, "2000", f"{name} disbursement", amount

        if txn_type == "marketing":
            account = self._weighted_choice({"6200": 0.30, "6210": 0.50, "6220": 0.20})
            name, _ = _GL_ACCOUNTS[account]
            amount = round(float(r.uniform(500.0, 50_000.0)), 2)
            return account, "1000", f"{name} spend", amount

        if txn_type == "logistics":
            account = self._weighted_choice({"6400": 0.70, "6410": 0.30})
            name, _ = _GL_ACCOUNTS[account]
            amount = round(float(r.uniform(2.99, 1_500.0)), 2)
            return account, "2000", f"{name} invoice", amount

        if txn_type == "rent_utilities":
            account = self._weighted_choice({"6100": 0.55, "6110": 0.30, "6120": 0.15})
            name, _ = _GL_ACCOUNTS[account]
            amount = round(float(r.uniform(1_000.0, 60_000.0)), 2)
            return account, "2000", f"{name} — monthly", amount

        if txn_type == "refunds":
            amount = round(float(r.uniform(10.0, 800.0)), 2)
            return "4100", "1000", "Customer refund issued", amount

        # cloud_saas
        account = self._weighted_choice({"6300": 0.50, "6310": 0.50})
        name, _ = _GL_ACCOUNTS[account]
        amount = round(float(r.uniform(200.0, 15_000.0)), 2)
        return account, "1000", f"{name} subscription", amount
