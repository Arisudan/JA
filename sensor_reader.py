#!/usr/bin/env python3
"""
npn_test_gpio.py

Simple test for 3-wire NPN proximity sensor connected to Raspberry Pi GPIO.

Wiring assumptions (Option A - direct pull-up):
 - Sensor Red  -> external 12V (or sensor-rated V+)
 - Sensor Black -> sensor 0V (common) -> Raspberry Pi GND (tie grounds)
 - Sensor Green -> Pi GPIO (e.g. BCM 17)
 - Configure Pi internal pull-up (3.3V) OR use external 10k pull-up from Pi 3.3V to GPIO

Behavior:
 - Idle: GPIO reads HIGH (pulled up)
 - Detect: sensor pulls output to GND -> GPIO reads LOW

Run:
  sudo python3 npn_test_gpio.py
"""
import RPi.GPIO as GPIO
import time
import argparse
from datetime import datetime

GPIO.setmode(GPIO.BCM)

parser = argparse.ArgumentParser()
parser.add_argument("--pin", type=int, default=17, help="BCM pin to read (default 17)")
parser.add_argument("--debounce", type=float, default=0.05, help="Debounce seconds")
args = parser.parse_args()

PIN = args.pin
DEBOUNCE = args.debounce

# Setup: use internal pull-up (safe) or disable if external pull-up present
GPIO.setup(PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)

print("NP N sensor test - reading pin BCM {}".format(PIN))
print("Make sure sensor powered externally and grounds are tied. Ctrl-C to exit.")
print()

last_state = GPIO.input(PIN)
print(f"[{datetime.now().isoformat()}] initial state: {'HIGH (open)' if last_state else 'LOW (sensed)'}")

try:
    while True:
        state = GPIO.input(PIN)
        if state != last_state:
            # very simple debounce
            time.sleep(DEBOUNCE)
            state2 = GPIO.input(PIN)
            if state2 == state:
                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                if state == GPIO.HIGH:
                    print(f"[{ts}] -> HIGH (output open, no metal)")
                else:
                    print(f"[{ts}] -> LOW  (sensor detected/OUTPUT pulled low)")
                last_state = state
        time.sleep(0.005)
except KeyboardInterrupt:
    print("\nExiting.")
finally:
    GPIO.cleanup()
