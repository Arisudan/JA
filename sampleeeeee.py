#!/usr/bin/env python3
"""
backend.py - Final Version: Pi 5 Ready, Spike-Proof, Smooth
(Added --force-polling and timestamp-based spike debounce. Other logic unchanged.)
"""
import asyncio
import websockets
import json
import time
import csv
import math
import threading
import argparse
import os
import datetime
from collections import deque
import statistics

# --- CONFIGURATION ---
SIMULATION_MODE = False 
DRIVE_CYCLE_FILE = 'drive_cycle.csv'
PORT = 8765
TOLERANCE_KMH = 2.0
LOG_DIR = 'logs' 
HALL_SENSOR_PIN = 17
WHEEL_CIRCUMFERENCE = 2.04 
MAGNETS_PER_WHEEL = 1
TIMEOUT_SECONDS = 1.5       

# --- FILTER SETTINGS ---
# Median Filter Window (Must be odd: 3, 5, 7)
FILTER_WINDOW_SIZE = 5
# Max allowed speed jump per update (km/h)
MAX_SPEED_JUMP = 5.0 
# Max possible speed (Hard Cap)
MAX_POSSIBLE_SPEED = 60.0

# Smoothing Factors
SMOOTH_RISE = 0.85
FAST_DROP   = 0.20
TICK_RATE   = 0.05 # 20Hz

# Force polling (can be set via CLI)
FORCE_POLLING = False

# Try to import GPIO
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except ImportError:
    GPIO_AVAILABLE = False

# --- GLOBAL STATE ---
profile_data = []
test_state = { "running": False, "start_time": 0, "elapsed": 0.0, "violations": 0, "actual_speed": 0.0, "is_outside": False }
clients = set()

# --- SPEED CALCULATION ---
last_pulse_time = 0.0
current_raw_speed = 0.0
speed_history = deque(maxlen=FILTER_WINDOW_SIZE)
filtered_speed = 0.0
display_speed = 0.0
pulse_lock = threading.Lock()
polling_active = False

# ---------- Debounce / spike-protection parameters ----------
# Compute a minimum plausible pulse interval (seconds) from geometry & max speed
# interval = wheel_circumference / speed_mps ; speed_mps = MAX_POSSIBLE_SPEED(km/h) / 3.6
MIN_PULSE_INTERVAL = WHEEL_CIRCUMFERENCE / (MAX_POSSIBLE_SPEED / 3.6) if MAX_POSSIBLE_SPEED > 0 else 0.02
# allow a safety factor (we ignore anything much shorter than the theoretical fastest)
MIN_PULSE_INTERVAL *= 0.4   # ignore pulses faster than 40% of the theoretical interval

# --- LOGGING SYSTEM ---
current_log_file = None
current_csv_writer = None
last_log_content = ""
last_logged_int_sec = -1
session_start_str = ""

def setup_logging():
    if not os.path.exists(LOG_DIR):
        try: os.makedirs(LOG_DIR); os.chmod(LOG_DIR, 0o777)
        except: pass

def start_new_log():
    global current_log_file, current_csv_writer, last_log_content, last_logged_int_sec, session_start_str
    now = datetime.datetime.now()
    session_start_str = now.strftime("%Y-%m-%d %H:%M:%S")
    timestamp_fname = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{LOG_DIR}/run_{timestamp_fname}.csv"
    last_logged_int_sec = -1 
    try:
        current_log_file = open(filename, 'w', newline='')
        current_csv_writer = csv.writer(current_log_file)
        header = ["Time", "Target", "Actual", "Upper_Limit", "Lower_Limit", "Violations"]
        current_csv_writer.writerow(["# Hero Drive Cycle Report"])
        current_csv_writer.writerow([f"# Start: {session_start_str}"])
        current_csv_writer.writerow([])
        current_csv_writer.writerow(header)
        current_log_file.flush()
        last_log_content = ",".join(header) + "\n"
        print(f"[LOG] Started: {filename}")
    except: pass

def log_data_point(t, tgt, act, up, lo, viol):
    global last_log_content, last_logged_int_sec
    current_int_sec = int(t)
    if current_int_sec > last_logged_int_sec:
        if current_csv_writer and current_log_file:
            try:
                row = [current_int_sec, f"{tgt:.1f}", f"{act:.1f}", f"{up:.1f}", f"{lo:.1f}", viol]
                current_csv_writer.writerow(row)
                current_log_file.flush() 
                os.fsync(current_log_file.fileno()) 
                last_log_content += ",".join(map(str, row)) + "\n"
                last_logged_int_sec = current_int_sec
            except: pass

def stop_logging():
    global current_log_file
    if current_log_file:
        try: current_log_file.close()
        except: pass
        current_log_file = None

# --- SENSOR LOGIC ---
def process_pulse():
    """
    Called to register a single pulse. Filters out spikes by time comparison.
    Keeps the same computation for speed as before.
    """
    global last_pulse_time, current_raw_speed
    current_time = time.monotonic()

    with pulse_lock:
        # If this is the very first pulse, initialize last_pulse_time and return
        if last_pulse_time == 0:
            last_pulse_time = current_time
            return

        delta_time = current_time - last_pulse_time

        # Ignore unrealistically fast pulses (spikes / bounces)
        if delta_time < MIN_PULSE_INTERVAL:
            # Optionally: occasional logging for debugging (commented out to avoid spam)
            # print(f"[HW] Ignored spike: dt={delta_time:.4f} < min={MIN_PULSE_INTERVAL:.4f}")
            return

        # Normal speed calculation
        if delta_time > 0:
            speed_mps = WHEEL_CIRCUMFERENCE / delta_time
            new_speed = speed_mps * 3.6
            if new_speed < MAX_POSSIBLE_SPEED * 1.05:  # a little headroom
                current_raw_speed = new_speed

        last_pulse_time = current_time

def gpio_callback(channel):
    """
    Interrupt callback must be tiny and non-blocking. Just schedule/record the pulse.
    Using a lock and timestamp guard inside process_pulse keeps this safe.
    """
    try:
        process_pulse()
    except Exception:
        pass

def polling_thread_func(pin):
    last_state = GPIO.input(pin)
    while polling_active:
        try:
            current_state = GPIO.input(pin)
            if last_state == 1 and current_state == 0:
                process_pulse()
            last_state = current_state
            time.sleep(0.001)
        except Exception:
            # keep polling alive even if a transient error occurs
            time.sleep(0.01)

def setup_hardware(pin):
    """
    Set up the GPIO. If FORCE_POLLING is True we skip add_event_detect entirely
    and start the polling thread (same behavior as fallback). This forces the
    same behavior across Pi4/Pi5.
    """
    global polling_active
    if not GPIO_AVAILABLE:
        print("[HW] GPIO not available.")
        return

    try:
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
    except Exception as e:
        print(f"[HW] GPIO setup error: {e}")
        return

    # If the user forced polling, start polling thread and return
    if FORCE_POLLING:
        print(f"[HW] FORCE_POLLING enabled — using polling on GPIO {pin} (min_interval={MIN_PULSE_INTERVAL:.3f}s)")
        polling_active = True
        threading.Thread(target=polling_thread_func, args=(pin,), daemon=True).start()
        return

    # Otherwise try interrupts, fall back to polling on failure
    try:
        GPIO.remove_event_detect(pin)
    except Exception:
        pass

    try:
        # use a modest bouncetime (ms). Keep it small because wheel pulses can be relatively quick.
        GPIO.add_event_detect(pin, GPIO.FALLING, callback=gpio_callback, bouncetime=10)
        print(f"[HW] Interrupts Active on GPIO {pin} (min_interval={MIN_PULSE_INTERVAL:.3f}s)")
    except Exception as e:
        print(f"[HW] Interrupt failed ({e}). Switching to Polling.")
        polling_active = True
        threading.Thread(target=polling_thread_func, args=(pin,), daemon=True).start()

def get_final_speed():
    global display_speed, filtered_speed, current_raw_speed
    
    if SIMULATION_MODE: return test_state.get("manual_speed", 0.0)
    
    if time.monotonic() - last_pulse_time > TIMEOUT_SECONDS:
        current_raw_speed = 0.0
    
    # Decay Logic
    time_since_pulse = time.monotonic() - last_pulse_time
    if time_since_pulse > 0.2:
        max_theoretical = (WHEEL_CIRCUMFERENCE / time_since_pulse) * 3.6
        if current_raw_speed > max_theoretical:
            current_raw_speed = max_theoretical

    # MEDIAN FILTER
    speed_history.append(current_raw_speed)
    if len(speed_history) < 3: return current_raw_speed
    median_val = statistics.median(speed_history)
    
    # RATE LIMITER
    diff = median_val - filtered_speed
    if diff > MAX_SPEED_JUMP: diff = MAX_SPEED_JUMP
    if diff < -MAX_SPEED_JUMP: diff = -MAX_SPEED_JUMP
    filtered_speed += diff
    
    # FINAL SMOOTHING
    target = filtered_speed
    diff_display = target - display_speed
    
    if diff_display < 0: factor = FAST_DROP
    else: factor = SMOOTH_RISE
        
    display_speed += diff_display * (1.0 - factor) # Simple low pass

    # LERP style smoothing (make drops fast, rises smooth)
    if target < display_speed:
        f = 0.8 # Fast drop (80% towards target per frame)
    else:
        f = 0.15 # Smooth rise (15% towards target per frame)
        
    display_speed = (display_speed * (1.0 - f)) + (target * f)
    
    if display_speed < 0.5: display_speed = 0.0
    
    return display_speed

# --- CSV UTILS ---
def load_profile():
    global profile_data
    profile_data = []
    try:
        with open(DRIVE_CYCLE_FILE, 'r') as f:
            has_header = csv.Sniffer().has_header(f.read(1024))
            f.seek(0)
            reader = csv.DictReader(f) if has_header else None
            if reader:
                for row in reader:
                    try:
                        t = float(row['time']); tgt = float(row['target'])
                        up = float(row.get('upper', tgt+TOLERANCE_KMH))
                        lo = float(row.get('lower', max(0, tgt-TOLERANCE_KMH)))
                        profile_data.append({'time':t, 'target':tgt, 'upper':up, 'lower':lo})
                    except: pass
            else:
                f.seek(0)
                for i, row in enumerate(csv.reader(f)):
                    if row: profile_data.append({'time': i*1.0, 'target': float(row[0]), 'upper': float(row[0])+TOLERANCE_KMH, 'lower': max(0, float(row[0])-TOLERANCE_KMH)})
    except: pass

def get_targets(t):
    if not profile_data: return 0,0,0
    if t >= profile_data[-1]['time']: return profile_data[-1]['target'], profile_data[-1]['upper'], profile_data[-1]['lower']
    for i in range(len(profile_data)-1):
        p1, p2 = profile_data[i], profile_data[i+1]
        if p1['time'] <= t <= p2['time']:
            r = (t - p1['time']) / (p2['time'] - p1['time']) if p2['time']!=p1['time'] else 0
            return (p1['target'] + (p2['target']-p1['target'])*r), (p1['upper'] + (p2['upper']-p1['upper'])*r), (p1['lower'] + (p2['lower']-p1['lower'])*r)
    return 0,0,0

async def handler(ws):
    clients.add(ws)
    try:
        times = [p['time'] for p in profile_data]
        targets = [p['target'] for p in profile_data]
        uppers = [p['upper'] for p in profile_data]
        lowers = [p['lower'] for p in profile_data]
        await ws.send(json.dumps({"type":"profile", "profile":{"time":times, "target":targets, "upper":uppers, "lower":lowers}}))
        async for msg in ws:
            m = json.loads(msg)
            cmd = m.get('cmd')
            if cmd == 'start':
                test_state['running'] = True
                test_state['start_time'] = time.time() - test_state['elapsed']
                start_new_log()
            elif cmd == 'stop':
                test_state['running'] = False
                stop_logging()
            elif cmd == 'reset':
                stop_logging()
                test_state.update({"running":False, "elapsed":0, "violations":0, "is_outside":False})
                await broadcast({"type":"reset"})
            elif cmd == 'download_log':
                end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await ws.send(json.dumps({
                    "type": "csv_download", 
                    "content": last_log_content,
                    "meta": { "start": session_start_str if session_start_str else "N/A", "end": end }}))
            elif cmd == 'sensor_status':
                active = get_final_speed() > 0.1
                await ws.send(json.dumps({
                    "type": "sensor_status",
                    "sensor_active": active,
                    "gpio_pin": HALL_SENSOR_PIN,
                    "gpio_level": "LOW" if active else "HIGH",
                    "current_speed": f"{get_final_speed():.1f}"
                }))
    except: pass
    finally: 
        try: clients.remove(ws)
        except: pass

async def broadcast(msg):
    if clients: await asyncio.gather(*[ws.send(json.dumps(msg)) for ws in clients], return_exceptions=True)

async def loop():
    while True:
        act = get_final_speed()
        test_state['actual_speed'] = act
        if test_state['running']:
            test_state['elapsed'] = time.time() - test_state['start_time']
            t = test_state['elapsed']
            tgt, up, lo = get_targets(t)
            outside = (act > up) or (act < lo)
            if t > 5.0 and outside and not test_state['is_outside']:
                test_state['violations'] += 1
            test_state['is_outside'] = outside
            log_data_point(t, tgt, act, up, lo, test_state['violations'])
            if profile_data and t >= profile_data[-1]['time']:
                test_state['running'] = False
                stop_logging()
                await broadcast({"type":"complete", "violations":test_state['violations']})
            await broadcast({"type":"update", "time":t, "target":tgt, "upper":up, "lower":lo, "actual":act, "violations":test_state['violations']})
        else:
            await broadcast({"type":"update", "time":test_state['elapsed'], "actual":act, "violations":test_state['violations']})
        await asyncio.sleep(TICK_RATE)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', default='drive_cycle.csv')
    parser.add_argument('--gpio-pin', type=int, default=17)
    parser.add_argument('--use-gpio', action='store_true')
    parser.add_argument('--force-polling', action='store_true', help='Force GPIO polling instead of interrupts')
    args = parser.parse_args()
    global DRIVE_CYCLE_FILE, HALL_SENSOR_PIN, FORCE_POLLING
    DRIVE_CYCLE_FILE = args.profile
    HALL_SENSOR_PIN = args.gpio_pin
    FORCE_POLLING = args.force_polling
    setup_logging()
    setup_hardware(HALL_SENSOR_PIN)
    load_profile()
    print(f"Server running on ws://0.0.0.0:{PORT} at 20Hz")
    asyncio.create_task(loop())
    async with websockets.serve(handler, "0.0.0.0", PORT) as server: await asyncio.Future()

if __name__ == "__main__":
    try: asyncio.run(main())
    except: 
        if GPIO_AVAILABLE:
            try: GPIO.cleanup()
            except: pass
