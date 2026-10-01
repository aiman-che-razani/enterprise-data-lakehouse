"""IoT sensor readings generator — 2,000,000 time-series readings (full scale)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

_SENSOR_TYPES = {
    "temperature_celsius": {
        "unit": "°C",
        "normal_range": (18.0, 26.0),
        "alert_threshold_low": 15.0,
        "alert_threshold_high": 30.0,
        "noise_std": 0.5,
        "interval_minutes": 15,
    },
    "humidity_pct": {
        "unit": "%",
        "normal_range": (35.0, 65.0),
        "alert_threshold_low": 20.0,
        "alert_threshold_high": 80.0,
        "noise_std": 2.0,
        "interval_minutes": 15,
    },
    "co2_ppm": {
        "unit": "ppm",
        "normal_range": (400.0, 1000.0),
        "alert_threshold_low": 300.0,
        "alert_threshold_high": 1500.0,
        "noise_std": 50.0,
        "interval_minutes": 30,
    },
    "foot_traffic_count": {
        "unit": "count",
        "normal_range": (0.0, 100.0),
        "alert_threshold_low": 0.0,
        "alert_threshold_high": 300.0,
        "noise_std": 10.0,
        "interval_minutes": 5,
    },
    "energy_kwh": {
        "unit": "kWh",
        "normal_range": (0.5, 8.0),
        "alert_threshold_low": 0.1,
        "alert_threshold_high": 15.0,
        "noise_std": 0.8,
        "interval_minutes": 60,
    },
    "refrigeration_temp_celsius": {
        "unit": "°C",
        "normal_range": (1.0, 4.0),
        "alert_threshold_low": -2.0,
        "alert_threshold_high": 8.0,
        "noise_std": 0.3,
        "interval_minutes": 10,
    },
    "noise_db": {
        "unit": "dB",
        "normal_range": (40.0, 70.0),
        "alert_threshold_low": 30.0,
        "alert_threshold_high": 90.0,
        "noise_std": 5.0,
        "interval_minutes": 30,
    },
    "air_quality_index": {
        "unit": "AQI",
        "normal_range": (0.0, 50.0),
        "alert_threshold_low": 0.0,
        "alert_threshold_high": 100.0,
        "noise_std": 3.0,
        "interval_minutes": 60,
    },
}

_SENSOR_TYPE_NAMES = list(_SENSOR_TYPES.keys())


class IotSensorsGenerator(BaseGenerator):
    domain = "iot_sensors"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.iot_sensors

    def _generate_record(self, index: int) -> dict[str, Any]:
        stores = self.registry.store_ids or [f"STR-{int(self.rng.integers(1, 250)):05d}"]
        store_id = self._choose(stores)

        sensor_type = self._choose(_SENSOR_TYPE_NAMES)
        meta = _SENSOR_TYPES[sensor_type]
        lo, hi = meta["normal_range"]

        # Sensor ID: stable per store × type combination
        sensor_num = hash(f"{store_id}-{sensor_type}") % 1000
        sensor_id = f"SEN-{store_id}-{sensor_type[:3].upper()}-{sensor_num:04d}"

        # Base reading with gaussian noise
        base_value = float(self.rng.uniform(lo, hi))
        noisy_value = base_value + float(self.rng.normal(0, meta["noise_std"]))

        # 2% chance of anomaly reading
        is_anomaly = self.rng.random() < 0.02
        if is_anomaly:
            anomaly_direction = 1 if self.rng.random() > 0.5 else -1
            noisy_value += anomaly_direction * meta["noise_std"] * float(self.rng.uniform(8, 20))

        reading_value = round(noisy_value, 3)
        alert_low = meta["alert_threshold_low"]
        alert_high = meta["alert_threshold_high"]
        alert_triggered = reading_value < alert_low or reading_value > alert_high

        # Timestamp aligned to sensor interval
        interval_min = meta["interval_minutes"]
        reading_dt = self._random_datetime()
        # Quantize to interval
        total_minutes = reading_dt.hour * 60 + reading_dt.minute
        quantized_minutes = (total_minutes // interval_min) * interval_min
        reading_dt = datetime(
            reading_dt.year,
            reading_dt.month,
            reading_dt.day,
            quantized_minutes // 60,
            quantized_minutes % 60,
            0,
            tzinfo=UTC,
        )

        # Battery and signal quality for IoT realism
        battery_pct = round(float(self.rng.uniform(10.0, 100.0)), 1)
        signal_rssi = int(self.rng.integers(-90, -30))

        return {
            "reading_id": f"IOT-{index + 1:012d}",
            "sensor_id": sensor_id,
            "store_id": store_id,
            "sensor_type": sensor_type,
            "value": reading_value,
            "unit": meta["unit"],
            "alert_triggered": alert_triggered,
            "alert_threshold_low": alert_low,
            "alert_threshold_high": alert_high,
            "is_anomaly": is_anomaly,
            "battery_pct": battery_pct,
            "signal_rssi_dbm": signal_rssi,
            "reading_quality": (
                "poor" if signal_rssi < -80 else "fair" if signal_rssi < -65 else "good"
            ),
            "firmware_version": f"v{int(self.rng.integers(1, 4))}.{int(self.rng.integers(0, 10))}.{int(self.rng.integers(0, 20))}",
            "reading_at": self._iso_dt(reading_dt),
            "ingested_at": self._iso_dt(
                reading_dt + timedelta(seconds=int(self.rng.integers(1, 30)))
            ),
        }
