"""
Docker HEALTHCHECK probe script.

Called by Docker every 120s. Checks if the heartbeat file (written by
health.py every health check cycle) is recent. If the heartbeat is stale
for >10 minutes (2 missed health cycles), exits with code 1 (unhealthy).

After 3 consecutive unhealthy checks, Docker kills and restarts the container.
"""

import sys
import time
from pathlib import Path

HEARTBEAT_FILE = Path("/app/data/heartbeat")
MAX_AGE_SECONDS = 600  # 10 minutes (2 missed 5-min health cycles)


def main():
    if not HEARTBEAT_FILE.exists():
        # No heartbeat yet — could be first startup, give benefit of the doubt
        # Docker's start-period (120s) covers initial startup
        print("HEALTHCHECK: No heartbeat file found")
        sys.exit(1)

    try:
        last_beat = float(HEARTBEAT_FILE.read_text().strip())
        age = time.time() - last_beat

        if age > MAX_AGE_SECONDS:
            print(f"HEALTHCHECK: Heartbeat stale ({age:.0f}s old, max {MAX_AGE_SECONDS}s)")
            sys.exit(1)

        print(f"HEALTHCHECK: OK (heartbeat {age:.0f}s ago)")
        sys.exit(0)

    except (ValueError, OSError) as e:
        print(f"HEALTHCHECK: Error reading heartbeat: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
