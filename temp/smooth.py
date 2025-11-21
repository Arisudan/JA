#!/usr/bin/env python3
"""
backend.py - Noise Filtered + Smooth Analog Physics
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

# --- CONFIGURATION ---
SIMULATION_MODE = False 
DRIVE_CYCLE_FILE = 'drive_cycle.csv'
PORT = 8765
TOLERANCE_KMH = 2.0
LOG_DIR = 'logs' 
HALL_SENSOR_PIN = 17
WHEEL_CIRCUMFERENCE = 2.04 
MAGNETS_PER_WHEEL = 1
TIMEOUT_SECONDS = 3.0       

# --- NOISE FILTER SETTINGS ---
DEBOUNCE_TIME = 0.05  # Ignore pulses faster than 50ms (Max readable speed ~145 km/h)
MAX_REALISTIC_SPEED = 120.0 # Ignore any spike above this (Physics Sanity Check)

# Try to import GPIO
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except ImportError:
    GPIO_AVAILABLE = False

# --- GLOBAL STATE ---
profile_data = []
test_state = { 
    "running": False, "start_time": 0, "elapsed": 0.0, 
    "violations": 0, "actual_speed": 0.0, "manual_speed": 0.0, 
    "is_outside": False 
}
clients = set()

# --- SPEED VARIABLES ---
last_pulse_time = 0.0
last_raw_speed = 0.0
display_speed = 0.0
pulse_lock = threading.Lock()
polling_active = False
last_loop_time = time.monotonic()

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
    filename = f"{LOG_DIR}/run_{now.strftime('%Y%m%d_%H%M%S')}.csv"
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
        print(f"[LOG] Logging to {filename}")
    except Exception as e: print(f"[LOG] Error: {e}")

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
    global current_log_file, current_csv_writer
    if current_log_file:
        try: current_log_file.close()
        except: pass
        current_log_file = None
        current_csv_writer = None

# --- SENSOR LOGIC (NOISE FILTERED) ---
def process_pulse():
    global last_pulse_time, last_raw_speed
    current_time = time.monotonic()
    
    with pulse_lock:
        delta_time = current_time - last_pulse_time
        
        # 1. DEBOUNCE: Ignore noise (pulses closer than 50ms)
        if delta_time > DEBOUNCE_TIME:
            if last_pulse_time != 0:
                speed_mps = WHEEL_CIRCUMFERENCE / delta_time
                new_raw_speed = speed_mps * 3.6
                
                # 2. SANITY CHECK: Ignore impossible speeds (e.g. > 120km/h)
                if new_raw_speed < MAX_REALISTIC_SPEED:
                    last_raw_speed = new_raw_speed
                    # print(f"[SENSOR] Pulse! Speed: {last_raw_speed:.1f}")
                else:
                    print(f"[FILTER] Ignored Noise Spike: {new_raw_speed:.1f} km/h")
            
            last_pulse_time = current_time

def gpio_callback(channel):
    process_pulse()

def polling_thread_func(pin):
    last_state = GPIO.input(pin)
    while polling_active:
        current_state = GPIO.input(pin)
        if last_state == 1 and current_state == 0:
            process_pulse()
        last_state = current_state
        time.sleep(0.001)

def setup_hardware(pin):
    global polling_active
    if not GPIO_AVAILABLE: return
    try:
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        try:
            GPIO.remove_event_detect(pin)
            GPIO.add_event_detect(pin, GPIO.FALLING, callback=gpio_callback, bouncetime=20)
            print(f"[HW] Interrupts Active on GPIO {pin}")
        except:
            print("[HW] Interrupt failed. Switching to Polling.")
            polling_active = True
            threading.Thread(target=polling_thread_func, args=(pin,), daemon=True).start()
    except Exception as e: print(f"[HW] Error: {e}")

def calculate_physics_speed():
    """
    Simulates analog needle physics to smooth out the graph.
    """
    global display_speed, last_raw_speed
    
    if SIMULATION_MODE: return test_state["manual_speed"]
    
    now = time.monotonic()
    time_since_pulse = now - last_pulse_time
    
    # 1. Predictive Decay (Coast down logic)
    if time_since_pulse > 0.1:
        max_theoretical = (WHEEL_CIRCUMFERENCE / time_since_pulse) * 3.6
        if last_raw_speed > max_theoretical:
            last_raw_speed = max_theoretical 

    # 2. Timeout (Stop)
    if time_since_pulse > TIMEOUT_SECONDS:
        last_raw_speed = 0.0
        
    # 3. Smooth Inertia (The "Heavy Needle" Effect)
    # Move 15% closer to target per frame
    diff = last_raw_speed - display_speed
    display_speed += diff * 0.15 
    
    return display_speed

# --- CSV LOADING & WEBSOCKET ---
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
        print(f"[DATA] Loaded {len(profile_data)} points")
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
                end_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await ws.send(json.dumps({
                    "type": "csv_download", 
                    "content": last_log_content,
                    "meta": { "start": session_start_str if session_start_str else "N/A", "end": end_time_str }
                }))
            elif cmd == 'sensor_status':
                active = calculate_physics_speed() > 0.1
                await ws.send(json.dumps({
                    "type": "sensor_status",
                    "sensor_active": active,
                    "gpio_pin": HALL_SENSOR_PIN,
                    "gpio_level": "LOW" if active else "HIGH",
                    "current_speed": f"{calculate_physics_speed():.1f}"
                }))
    except: pass
    finally: clients.remove(ws)

async def broadcast(msg):
    if clients: await asyncio.gather(*[ws.send(json.dumps(msg)) for ws in clients], return_exceptions=True)

async def loop():
    while True:
        act = calculate_physics_speed()
        test_state['actual_speed'] = act
        
        if test_state['running']:
            test_state['elapsed'] = time.time() - test_state['start_time']
            t = test_state['elapsed']
            tgt, up, lo = get_targets(t)
            outside = (act > up) or (act < lo)
            
            if t > 5.0:
                if outside and not test_state['is_outside']:
                    test_state['violations'] += 1
                    await broadcast({"type":"violation", "time":t, "violations":test_state['violations'], "side":("upper" if act>up else "lower"), "actual":act})
            test_state['is_outside'] = outside
            
            log_data_point(t, tgt, act, up, lo, test_state['violations'])
            
            if profile_data and t >= profile_data[-1]['time']:
                test_state['running'] = False
                stop_logging()
                await broadcast({"type":"complete", "violations":test_state['violations']})
                
            await broadcast({"type":"update", "time":t, "target":tgt, "upper":up, "lower":lo, "actual":act, "violations":test_state['violations']})
        else:
            await broadcast({"type":"update", "time":test_state['elapsed'], "actual":act, "violations":test_state['violations']})
        
        await asyncio.sleep(0.1)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', default='drive_cycle.csv')
    parser.add_argument('--gpio-pin', type=int, default=17)
    parser.add_argument('--use-gpio', action='store_true')
    args = parser.parse_args()

    global DRIVE_CYCLE_FILE, HALL_SENSOR_PIN, SIMULATION_MODE
    DRIVE_CYCLE_FILE = args.profile
    HALL_SENSOR_PIN = args.gpio_pin
    if args.use_gpio: SIMULATION_MODE = False
    
    setup_logging()
    setup_hardware(HALL_SENSOR_PIN)
    load_profile()
    print(f"Server running on ws://0.0.0.0:{PORT}")
    
    asyncio.create_task(loop())
    async with websockets.serve(handler, "0.0.0.0", PORT) as server:
        await asyncio.Future()

if __name__ == "__main__":
    try: asyncio.run(main())
    except: 
        if GPIO_AVAILABLE: GPIO.cleanup()
