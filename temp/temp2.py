#!/usr/bin/env python3
"""
backend.py - Fixed for Websockets v14+ (Removed 'path' argument)
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

# --- VEHICLE PHYSICS ---
HALL_SENSOR_PIN = 17
WHEEL_CIRCUMFERENCE = 1.94  
MAGNETS_PER_WHEEL = 1
TIMEOUT_SECONDS = 3.0       

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

# --- SPEED CALCULATION ---
last_pulse_time = 0.0
current_speed_kmh = 0.0
pulse_lock = threading.Lock()
polling_active = False

# --- LOGGING SYSTEM ---
current_log_file = None
current_csv_writer = None
last_log_content = "" 

def setup_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def start_new_log():
    global current_log_file, current_csv_writer, last_log_content
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{LOG_DIR}/run_{timestamp}.csv"
    try:
        current_log_file = open(filename, 'w', newline='')
        current_csv_writer = csv.writer(current_log_file)
        current_csv_writer.writerow(["Time", "Target", "Actual", "Upper_Limit", "Lower_Limit", "Violations"])
        print(f"[LOG] Started logging to {filename}")
        last_log_content = "Time,Target,Actual,Upper_Limit,Lower_Limit,Violations\n"
    except Exception as e:
        print(f"[LOG] Error creating log file: {e}")

def log_data_point(t, tgt, act, up, lo, viol):
    global last_log_content
    if current_csv_writer and current_log_file:
        try:
            row = [f"{t:.2f}", f"{tgt:.1f}", f"{act:.1f}", f"{up:.1f}", f"{lo:.1f}", viol]
            current_csv_writer.writerow(row)
            last_log_content += ",".join(map(str, row)) + "\n"
        except: pass

def stop_logging():
    global current_log_file, current_csv_writer
    if current_log_file:
        try:
            current_log_file.flush()
            current_log_file.close()
            print("[LOG] Log file saved and closed.")
        except: pass
        current_log_file = None
        current_csv_writer = None

# --- SENSOR LOGIC ---
def process_pulse():
    global last_pulse_time, current_speed_kmh
    current_time = time.time()
    with pulse_lock:
        delta_time = current_time - last_pulse_time
        if delta_time > 0.01: 
            if last_pulse_time != 0:
                speed_mps = WHEEL_CIRCUMFERENCE / delta_time
                new_speed = speed_mps * 3.6
                current_speed_kmh = (current_speed_kmh * 0.3) + (new_speed * 0.7)
            last_pulse_time = current_time
            print(f"[SENSOR] Pulse! {current_speed_kmh:.1f} km/h")

def gpio_callback(channel):
    process_pulse()

def polling_thread_func(pin):
    last_state = GPIO.input(pin)
    print("[HW] Polling Mode Active")
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

def get_current_speed():
    global current_speed_kmh, last_pulse_time
    if SIMULATION_MODE: return test_state["manual_speed"]
    if time.time() - last_pulse_time > TIMEOUT_SECONDS:
        with pulse_lock: current_speed_kmh = 0.0
    return current_speed_kmh

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
                        profile_data.append({
                            'time': float(row['time']),
                            'target': float(row['target']),
                            'upper': float(row.get('upper', float(row['target'])+2)),
                            'lower': float(row.get('lower', max(0, float(row['target'])-2)))
                        })
                    except: pass
            else:
                f.seek(0)
                for i, row in enumerate(csv.reader(f)):
                    if row: profile_data.append({'time': i*1.0, 'target': float(row[0]), 'upper': float(row[0])+2, 'lower': max(0, float(row[0])-2)})
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

# --- FIXED HANDLER (Removed 'path' argument) ---
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
            elif cmd == 'manual_speed':
                test_state['manual_speed'] = float(m.get('speed', 0))
            elif cmd == 'download_log':
                await ws.send(json.dumps({"type": "csv_download", "content": last_log_content}))
                
    except: pass
    finally: clients.remove(ws)

async def broadcast(msg):
    if clients: await asyncio.gather(*[ws.send(json.dumps(msg)) for ws in clients], return_exceptions=True)

async def loop():
    while True:
        act = get_current_speed()
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
