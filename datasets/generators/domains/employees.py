"""Employee data generator — 5,000 records (full scale)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

_DEPARTMENTS = [
    "Engineering",
    "Data & Analytics",
    "Product",
    "Marketing",
    "Sales",
    "Finance",
    "Operations",
    "Customer Service",
    "Human Resources",
    "Legal",
    "Supply Chain",
    "Store Operations",
]

_LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "D1", "D2", "VP", "C-Suite"]

_LEVEL_WEIGHTS = {
    "IC1": 0.10,
    "IC2": 0.20,
    "IC3": 0.22,
    "IC4": 0.15,
    "IC5": 0.08,
    "M1": 0.10,
    "M2": 0.07,
    "D1": 0.04,
    "D2": 0.02,
    "VP": 0.015,
    "C-Suite": 0.005,
}

# Annual salary range (USD) by level
_SALARY_RANGE = {
    "IC1": (45_000, 75_000),
    "IC2": (70_000, 100_000),
    "IC3": (95_000, 140_000),
    "IC4": (130_000, 185_000),
    "IC5": (175_000, 260_000),
    "M1": (110_000, 165_000),
    "M2": (155_000, 225_000),
    "D1": (200_000, 310_000),
    "D2": (280_000, 420_000),
    "VP": (350_000, 600_000),
    "C-Suite": (600_000, 2_000_000),
}

_EMPLOYMENT_TYPE = {"full_time": 0.82, "part_time": 0.12, "contractor": 0.06}
_GENDERS = {"M": 0.48, "F": 0.48, "Non-binary": 0.03, "Prefer not to say": 0.01}


_BRONZE_COLUMNS = (
    "employee_id",
    "first_name",
    "last_name",
    "email",
    "department",
    "job_title",
    "level",
    "manager_id",
    "store_id",
    "base_salary",
    "bonus_pct",
    "hire_date",
    "termination_date",
    "is_active",
    "supplier_id",
    "created_at",
    "updated_at",
)


class EmployeesGenerator(BaseGenerator):
    domain = "employees"
    output_format = "csv"

    def record_count(self) -> int:
        return self.config.counts.employees

    def _generate_record(self, index: int) -> dict[str, Any]:
        employee_id = f"EMP-{index + 1:06d}"
        level = self._weighted_choice(_LEVEL_WEIGHTS)
        department = self._choose(_DEPARTMENTS)
        employment_type = self._weighted_choice(_EMPLOYMENT_TYPE)

        lo, hi = _SALARY_RANGE[level]
        base_salary = int(self.rng.integers(lo, hi + 1))
        bonus_pct = {
            "IC1": 0.05,
            "IC2": 0.07,
            "IC3": 0.10,
            "IC4": 0.15,
            "IC5": 0.20,
            "M1": 0.15,
            "M2": 0.20,
            "D1": 0.25,
            "D2": 0.35,
            "VP": 0.45,
            "C-Suite": 0.80,
        }[level]

        hire_date = self._random_date(end=self.config.end_date)
        dob = self.fake.date_of_birth(minimum_age=22, maximum_age=65)

        record: dict[str, Any] = {
            "employee_id": employee_id,
            "first_name": self.fake.first_name(),
            "last_name": self.fake.last_name(),
            "email": self.fake.company_email(),
            "phone": self.fake.phone_number(),
            "date_of_birth": self._iso_date(dob),
            "gender": self._weighted_choice(_GENDERS),
            "department": department,
            "job_title": f"{level} {department} Specialist",
            "level": level,
            "employment_type": employment_type,
            "base_salary": base_salary,
            "target_bonus_pct": bonus_pct,
            "manager_id": (
                f"EMP-{int(self.rng.integers(1, max(2, index))):06d}"
                if index > 0 and level not in ("C-Suite",)
                else None
            ),
            "store_id": (
                self._choose(self.registry.store_ids)
                if department == "Store Operations" and self.registry.store_ids
                else None
            ),
            "is_active": bool(self.rng.random() > 0.08),
            "hire_date": self._iso_date(hire_date),
            "termination_date": (
                self._iso_date(hire_date + timedelta(days=int(self.rng.integers(180, 1095))))
                if self.rng.random() < 0.08
                else None
            ),
            "created_at": self._iso_dt(self._random_datetime(end=self.config.start_date)),
            "updated_at": self._iso_dt(self._random_datetime()),
        }

        self.registry.employee_ids.append(employee_id)
        self.registry.employee_departments[employee_id] = department
        # Emit exactly the Bronze contract (spark/schemas/bronze_schemas.py): names and column order.
        # Phone, date of birth, gender and employment type are not part of it (and are PII).
        record["bonus_pct"] = record.pop("target_bonus_pct")
        record["supplier_id"] = None
        return {k: record[k] for k in _BRONZE_COLUMNS}
