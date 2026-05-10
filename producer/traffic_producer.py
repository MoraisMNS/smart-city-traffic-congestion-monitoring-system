"""
Smart City Traffic Sensor Producer
===================================
Simulates 4 traffic junctions in Colombo sending real-time sensor data to Kafka.
Each sensor emits: {sensor_id, timestamp, vehicle_count, avg_speed} every second.

Critical Traffic Events: Injected randomly (~5% of messages) to test alert pipeline.
"""

import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging Setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("TrafficProducer")

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER      = "kafka:9092"
TOPIC_TRAFFIC     = "traffic-raw"
EMIT_INTERVAL_SEC = 1       # seconds between messages per sensor
CRITICAL_THRESHOLD_KMPH = 10  # below this => Critical Traffic alert

# Colombo junctions with realistic base traffic profiles
JUNCTIONS = {
    "J001": {"name": "Galle Face - Marine Drive",    "base_speed": 45, "base_count": 30},
    "J002": {"name": "Pettah - Manning Market",      "base_speed": 35, "base_count": 50},
    "J003": {"name": "Colombo 3 - Union Place",      "base_speed": 50, "base_count": 20},
    "J004": {"name": "Nugegoda - High Level Road",   "base_speed": 40, "base_count": 40},
}

# ── Helper Functions ───────────────────────────────────────────────────────────

def get_time_of_day_factor(hour: int) -> float:
    """Return a congestion multiplier based on time of day."""
    if 7 <= hour <= 9:    # Morning rush
        return 2.0
    elif 17 <= hour <= 19: # Evening rush
        return 1.8
    elif 12 <= hour <= 13: # Lunch hour
        return 1.3
    elif 0 <= hour <= 5:   # Late night
        return 0.3
    return 1.0


def generate_reading(sensor_id: str, force_critical: bool = False) -> dict:
    """Generate a single sensor reading for a junction."""
    junction    = JUNCTIONS[sensor_id]
    hour        = datetime.now().hour
    factor      = get_time_of_day_factor(hour)

    if force_critical:
        # Simulate severe congestion / accident scenario
        avg_speed     = round(random.uniform(2, CRITICAL_THRESHOLD_KMPH - 0.5), 1)
        vehicle_count = random.randint(80, 120)
        logger.warning(
            f"[CRITICAL INJECT] {sensor_id} ({junction['name']}) | "
            f"speed={avg_speed} km/h | count={vehicle_count}"
        )
    else:
        noise         = random.gauss(0, 5)
        avg_speed     = max(5, round(junction["base_speed"] / factor + noise, 1))
        vehicle_count = max(1, int(junction["base_count"] * factor + random.gauss(0, 5)))

    return {
        "sensor_id":     sensor_id,
        "junction_name": junction["name"],
        "timestamp":     datetime.utcnow().isoformat() + "Z",
        "vehicle_count": vehicle_count,
        "avg_speed":     avg_speed,
        "is_critical":   avg_speed < CRITICAL_THRESHOLD_KMPH,
    }


def create_producer() -> KafkaProducer:
    """Initialise Kafka producer with retries."""
    retries = 10
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Broker not ready (attempt {attempt}/{retries}). Retrying in 5s …")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


# ── Main Loop ──────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("  Smart City Traffic Producer — Colombo Junction Monitor")
    logger.info("=" * 60)
    logger.info(f"Topic        : {TOPIC_TRAFFIC}")
    logger.info(f"Emit Interval: {EMIT_INTERVAL_SEC}s per sensor")
    logger.info(f"Junctions    : {', '.join(JUNCTIONS.keys())}")
    logger.info("=" * 60)

    producer = create_producer()
    msg_count = 0

    try:
        while True:
            for sensor_id in JUNCTIONS:
                # Inject a critical traffic event ~5% of the time (controlled randomness)
                force_critical = random.random() < 0.05

                reading = generate_reading(sensor_id, force_critical=force_critical)

                producer.send(
                    topic=TOPIC_TRAFFIC,
                    key=sensor_id,
                    value=reading,
                )
                msg_count += 1

                status = "⚠  CRITICAL" if reading["is_critical"] else "✓  Normal  "
                logger.info(
                    f"[{status}] {sensor_id} | speed={reading['avg_speed']:5.1f} km/h | "
                    f"count={reading['vehicle_count']:3d} | msg#{msg_count}"
                )

            producer.flush()
            time.sleep(EMIT_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.close()
        logger.info(f"Total messages sent: {msg_count}")


if __name__ == "__main__":
    main()