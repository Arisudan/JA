import RPi.GPIO as GPIO
import time

# -------------------------------------------------
# CONFIGURATION (Update if needed)
# -------------------------------------------------
GPIO_PIN = 17           # Proximity sensor connected to GPIO17
PULSES_PER_REV = 1      # 1 pulse per wheel revolution (single magnet/proximity hit)
WHEEL_CIRC = 1.94       # Wheel circumference in meters (80/100-18 tyre)
DEBOUNCE_MS = 10        # Debounce filtering to avoid false triggers

# -------------------------------------------------
# GLOBAL VARIABLES
# -------------------------------------------------
last_time = None
speed_kmh = 0.0

# -------------------------------------------------
# SETUP
# -------------------------------------------------
GPIO.setmode(GPIO.BCM)
GPIO.setup(GPIO_PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)

print("------------------------------------------------------")
print("  REAL-TIME VEHICLE SPEED MONITOR (Raspberry Pi 4B)   ")
print("------------------------------------------------------")
print(f"Using GPIO Pin: {GPIO_PIN}")
print(f"Circumference: {WHEEL_CIRC} m")
print("Press CTRL+C to stop")
print("------------------------------------------------------")

# -------------------------------------------------
# CALLBACK: Triggered on every pulse
# -------------------------------------------------
def pulse_callback(channel):
    global last_time, speed_kmh

    current_time = time.time()

    if last_time is not None:
        dt = current_time - last_time  # time between pulses in seconds

        if dt > 0:
            rev_per_sec = 1.0 / dt
            speed_m_s = rev_per_sec * WHEEL_CIRC
            speed_kmh = speed_m_s * 3.6

            print(f"Speed: {speed_kmh:.2f} km/h")

    last_time = current_time


# -------------------------------------------------
# EVENT DETECTION
# -------------------------------------------------
GPIO.add_event_detect(
    GPIO_PIN,
    GPIO.FALLING,            # NPN sensor pulls to ground → falling edge
    callback=pulse_callback,
    bouncetime=DEBOUNCE_MS
)

# -------------------------------------------------
# MAIN LOOP: keeps program alive
# -------------------------------------------------
try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nExiting...")

finally:
    GPIO.cleanup()
    print("GPIO cleaned. Program ended.")
