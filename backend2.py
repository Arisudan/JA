#!/usr/bin/env python3
import asyncio
import websockets
import json
import time
import csv
import math

# --- CONFIGURATION ---
# Set to False ONLY when you connect the real sensor
SIMULATION_MODE = True 
DRIVE_CYCLE_FILE = 'drive_cycle.csv'
PORT = 8765
TOLERANCE_KMH = 2.0
HALL_SENSOR_PIN = 17
WHEEL_CIRCUMFERENCE = 1.94 
MAGNETS_PER_WHEEL = 1

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
pulse_count = 0
last_calc_time = time.time()

def count_pulse(channel):
    global pulse_count
    pulse_count += 1

def setup_hardware():
    if SIMULATION_MODE or not GPIO_AVAILABLE:
        print("[HW] Running in SIMULATION MODE (Use Slider)")
        return
    try:
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(HALL_SENSOR_PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        GPIO.add_event_detect(HALL_SENSOR_PIN, GPIO.FALLING, callback=count_pulse, bouncetime=20)
        print(f"[HW] GPIO {HALL_SENSOR_PIN} Active")
    except Exception as e:
        print(f"[HW] Error: {e}")

def get_speed():
    global pulse_count, last_calc_time
    if SIMULATION_MODE: 
        return test_state["manual_speed"]
    
    now = time.time()
    dt = now - last_calc_time
    if dt <= 0: return test_state["actual_speed"]
    
    revolutions = pulse_count / MAGNETS_PER_WHEEL
    rpm = (revolutions / dt) * 60
    speed = (rpm * WHEEL_CIRCUMFERENCE * 60) / 1000
    
    pulse_count = 0
    last_calc_time = now
    return speed

def load_profile():
    global profile_data
    profile_data = []
    try:
        with open(DRIVE_CYCLE_FILE, 'r') as f:
            # Check if header exists
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
                # Fallback for simple list of numbers
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if row:
                        tgt = float(row[0])
                        profile_data.append({'time': i*1.0, 'target': tgt, 'upper': tgt+2, 'lower': max(0, tgt-2)})
                        
        print(f"[DATA] Loaded {len(profile_data)} points")
    except Exception as e:
        print(f"[DATA] Error loading CSV: {e}")
        # Emergency fallback data
        for i in range(60):
            tgt = 10 if 10 < i < 50 else 0
            profile_data.append({'time':i, 'target':tgt, 'upper':tgt+2, 'lower':max(0,tgt-2)})

def get_target_at_time(t):
    if not profile_data: return 0, 0, 0
    if t >= profile_data[-1]['time']: 
        last = profile_data[-1]
        return last['target'], last['upper'], last['lower']
    
    # Linear interpolation
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

async def handler(websocket, path):
    clients.add(websocket)
    try:
        # Send graph data immediately
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
                
    except: pass
    finally: clients.remove(websocket)

async def broadcast(msg):
    if clients: 
        await asyncio.gather(*[ws.send(json.dumps(msg)) for ws in clients], return_exceptions=True)

async def loop():
    while True:
        actual = get_speed()
        test_state['actual_speed'] = actual
        
        if test_state['running']:
            test_state['elapsed'] = time.time() - test_state['start_time']
            t = test_state['elapsed']
            tgt, up, lo = get_target_at_time(t)
            
            # Violation Logic
            currently_outside = (actual > up) or (actual < lo)
            
            # Grace period: 5 seconds
            if t > 5.0:
                if currently_outside and not test_state['is_outside']:
                    test_state['violations'] += 1
                    side = "upper" if actual > up else "lower"
                    # Send immediate alert
                    await broadcast({
                        "type": "violation", 
                        "time": t, 
                        "violations": test_state['violations'], 
                        "side": side, 
                        "actual": actual
                    })
            
            test_state['is_outside'] = currently_outside
            
            # Check end
            if profile_data and t >= profile_data[-1]['time']:
                test_state['running'] = False
                await broadcast({"type": "complete", "violations": test_state['violations']})
            
            # Standard Update
            await broadcast({
                "type": "update", "time": t, "target": tgt, 
                "upper": up, "lower": lo, "actual": actual, 
                "violations": test_state['violations']
            })
        else:
             # Send idle update (so slider works)
             await broadcast({
                "type": "update", "time": test_state['elapsed'], 
                "actual": actual, "violations": test_state['violations']
            })
            
        await asyncio.sleep(0.2)

async def main():
    setup_hardware()
    load_profile()
    print("Server started on ws://0.0.0.0:8765")
    asyncio.create_task(loop())
    async with websockets.serve(handler, "0.0.0.0", 8765):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
