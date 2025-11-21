#!/usr/bin/env python3
"""
backend.py - Optimized for NPN Sensor & Real-time Speed
"""
import asyncio
import websockets
import json
import time
import csv
import math
import threading
import argparse
import signal
import sys

# --- CONFIGURATION ---
# Set to False ONLY when you connect the real sensor
SIMULATION_MODE = False 
DRIVE_CYCLE_FILE = 'drive_cycle.csv'
PORT = 8765
TOLERANCE_KMH = 2.0

# --- VEHICLE PHYSICS ---
HALL_SENSOR_PIN = 17
WHEEL_CIRCUMFERENCE = 1.94  # meters (Standard bike wheel)
MAGNETS_PER_WHEEL = 1
TIMEOUT_SECONDS = 3.0       # If no pulse for 3s, speed is 0

# Try to import GPIO
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except ImportError:
    GPIO_AVAILABLE = False

# --- GLOBAL STATE ---
profile_data = []
test_state = { 
    "running": False, 
    "start_time": 0, 
    "elapsed": 0.0, 
    "violations": 0, 
    "actual_speed": 0.0, 
    "manual_speed": 0.0, 
    "is_outside": False 
}
clients = set()

# --- SPEED CALCULATION VARIABLES ---
last_pulse_time = 0.0
current_speed_kmh = 0.0
pulse_lock = threading.Lock()

# --- HARDWARE INTERRUPT ---
def gpio_callback(channel):
    """Called instantly when sensor detects metal (Falling Edge)"""
    global last_pulse_time, current_speed_kmh
    
    current_time = time.time()
    
    with pulse_lock:
        # Calculate time since last pulse
        delta_time = current_time - last_pulse_time
        
        # Debounce: Ignore crazy fast pulses (noise) < 10ms
        if delta_time > 0.01:
            # Calculate Speed: Distance / Time
            # Speed (m/s) = Circumference / Time_Diff
            if last_pulse_time != 0:
                speed_mps = WHEEL_CIRCUMFERENCE / delta_time
                new_speed = speed_mps * 3.6  # Convert to km/h
                
                # Simple smoothing (average with previous) to stop jumping
                current_speed_kmh = (current_speed_kmh * 0.3) + (new_speed * 0.7)
            
            last_pulse_time = current_time
            print(f"[SENSOR] Pulse! Delta: {delta_time:.3f}s | Speed: {current_speed_kmh:.1f} km/h")

def setup_hardware(pin):
    if not GPIO_AVAILABLE:
        print("[HW] GPIO library not found. Using SIMULATION.")
        return

    try:
        GPIO.setmode(GPIO.BCM)
        # PUD_UP is critical for NPN sensors (pulls pin HIGH so sensor can pull LOW)
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        
        # Remove old events if any
        try: GPIO.remove_event_detect(pin)
        except: pass
        
        # Add Interrupt: Listen for Falling Edge (High -> Low)
        GPIO.add_event_detect(pin, GPIO.FALLING, callback=gpio_callback, bouncetime=20)
        
        print(f"[HW] Sensor Ready on GPIO {pin}. Waiting for wheel spin...")
    except Exception as e:
        print(f"[HW] Error setting up GPIO: {e}")

def get_current_speed():
    global current_speed_kmh, last_pulse_time
    
    if SIMULATION_MODE: 
        return test_state["manual_speed"]
    
    # Check for timeout (bike stopped)
    if time.time() - last_pulse_time > TIMEOUT_SECONDS:
        with pulse_lock:
            current_speed_kmh = 0.0
            
    return current_speed_kmh

# --- CSV LOADER ---
def load_profile():
    global profile_data
    profile_data = []
    try:
        with open(DRIVE_CYCLE_FILE, 'r') as f:
            # Handle headers or no headers
            has_header = csv.Sniffer().has_header(f.read(1024))
            f.seek(0)
            
            if has_header:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        t = float(row['time'])
                        tgt = float(row['target'])
                        up = float(row['upper']) if 'upper' in row else tgt + TOLERANCE_KMH
                        lo = float(row['lower']) if 'lower' in row else max(0, tgt - TOLERANCE_KMH)
                        profile_data.append({'time':t, 'target':tgt, 'upper':up, 'lower':lo})
                    except: pass
            else:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if row:
                        tgt = float(row[0])
                        profile_data.append({'time': i*1.0, 'target': tgt, 'upper': tgt+2, 'lower': max(0, tgt-2)})
        print(f"[DATA] Loaded {len(profile_data)} points")
    except Exception as e:
        print(f"[DATA] Error loading CSV: {e}")
        # Fallback data
        for i in range(60):
            tgt = 15 if 10 < i < 50 else 0
            profile_data.append({'time':i, 'target':tgt, 'upper':tgt+2, 'lower':max(0,tgt-2)})

# --- INTERPOLATION ---
def get_target_at_time(t):
    if not profile_data: return 0, 0, 0
    if t >= profile_data[-1]['time']: 
        last = profile_data[-1]
        return last['target'], last['upper'], last['lower']
    
    for i in range(len(profile_data)-1):
        p1 = profile_data[i]
        p2 = profile_data[i+1]
        if p1['time'] <= t <= p2['time']:
            ratio = (t - p1['time']) / (p2['time'] - p1['time']) if (p2['time'] != p1['time']) else 0
            tgt = p1['target'] + (p2['target'] - p1['target']) * ratio
            up = p1['upper'] + (p2['upper'] - p1['upper']) * ratio
            lo = p1['lower'] + (p2['lower'] - p1['lower']) * ratio
            return tgt, up, lo
    return 0, 0, 0

# --- WEBSOCKET HANDLER ---
async def handler(websocket, path):
    clients.add(websocket)
    try:
        # Send profile data
        times = [p['time'] for p in profile_data]
        targets = [p['target'] for p in profile_data]
        uppers = [p['upper'] for p in profile_data]
        lowers = [p['lower'] for p in profile_data]
        
        await websocket.send(json.dumps({
            "type": "profile", 
            "profile": {"time": times, "target": targets, "upper": uppers, "lower": lowers}
        }))
        
        async for message in websocket:
            msg = json.loads(message)
            cmd = msg.get('cmd')
            
            if cmd == 'start':
                test_state['running'] = True
                test_state['start_time'] = time.time() - test_state['elapsed']
                print("CMD: Start")
            elif cmd == 'stop': 
                test_state['running'] = False
            elif cmd == 'reset':
                test_state.update({"running": False, "elapsed": 0, "violations": 0, "is_outside": False})
                await broadcast({"type": "reset"})
            elif cmd == 'manual_speed': 
                test_state['manual_speed'] = float(msg.get('speed', 0))
            elif cmd == 'set_mode':
                global SIMULATION_MODE
                SIMULATION_MODE = (msg.get('mode') == 'manual')
                print(f"CMD: Mode set to {msg.get('mode')}")
                
    except: pass
    finally: clients.remove(websocket)

async def broadcast(msg):
    if clients: 
        await asyncio.gather(*[ws.send(json.dumps(msg)) for ws in clients], return_exceptions=True)

async def loop():
    while True:
        actual = get_current_speed()
        test_state['actual_speed'] = actual
        
        if test_state['running']:
            test_state['elapsed'] = time.time() - test_state['start_time']
            t = test_state['elapsed']
            tgt, up, lo = get_target_at_time(t)
            
            currently_outside = (actual > up) or (actual < lo)
            
            # 5 Second Grace Period
            if t > 5.0:
                if currently_outside and not test_state['is_outside']:
                    test_state['violations'] += 1
                    side = "upper" if actual > up else "lower"
                    await broadcast({
                        "type": "violation", 
                        "time": t, 
                        "violations": test_state['violations'], 
                        "side": side, 
                        "actual": actual
                    })
            
            test_state['is_outside'] = currently_outside
            
            if profile_data and t >= profile_data[-1]['time']:
                test_state['running'] = False
                await broadcast({"type": "complete", "violations": test_state['violations']})
            
            await broadcast({
                "type": "update", "time": t, "target": tgt, 
                "upper": up, "lower": lo, "actual": actual, 
                "violations": test_state['violations']
            })
        else:
             # Send updates even when stopped so we can see speed
             await broadcast({
                "type": "update", "time": test_state['elapsed'], 
                "actual": actual, "violations": test_state['violations']
            })
            
        await asyncio.sleep(0.1) # 10Hz update rate

async def main():
    # Parse Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', default='drive_cycle.csv')
    parser.add_argument('--gpio-pin', type=int, default=17)
    parser.add_argument('--use-gpio', action='store_true')
    args = parser.parse_args()

    global DRIVE_CYCLE_FILE, HALL_SENSOR_PIN, SIMULATION_MODE
    DRIVE_CYCLE_FILE = args.profile
    HALL_SENSOR_PIN = args.gpio_pin
    
    # If user adds --use-gpio flag, disable simulation
    if args.use_gpio:
        SIMULATION_MODE = False
    
    setup_hardware(HALL_SENSOR_PIN)
    load_profile()
    
    print(f"Server started on ws://0.0.0.0:{PORT}")
    asyncio.create_task(loop())
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt:
        if GPIO_AVAILABLE: GPIO.cleanup()
        print("\nStopped.")
```
