"""Backward-compatible re-export — the sensor now lives in ``sensors``."""

from sensors.watermark_sensor import WatermarkSensor

__all__ = ["WatermarkSensor"]
