#!/usr/bin/env python3
"""
backend.py

Counts a violation EACH time actual speed crosses inside->outside (upper or lower)
throughout the entire drive-cycle. Emits "crossed":true and "cross_side":"upper"/"lower"
in update messages so frontend can mark it.

Usage examples:
  # manual mode (no GPIO)
  python3 backend.py --profile drive_cycle.csv --rebase --debounce 0.0 --debug

  # with GPIO on Raspberry Pi (run with sudo)
  sudo python3 backend.py --profile drive_cycle.csv --rebase --use-gpio --gpio-pin 17 --circ 1.94 --debounce 0.05 --debug

Dependencies:
  pip install pandas numpy websockets
"""

import argparse, asyncio, csv, json, os, signal, sys, time
from collections import deque
from datetime import datetime
import datetime as dt
import threading

import numpy as np
import pandas as pd
import websockets

# Optional GPIO (RPi)
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
    print("[GPIO] RPi.GPIO library loaded - Real Raspberry Pi hardware detected")
except Exception:
    GPIO_AVAILABLE = False
    print("[GPIO] RPi.GPIO not available - Running in simulation mode (Windows/Linux without GPIO)")

# Defaults
PORT = 8765
TICK_HZ = 10  # Increased from 5 to 10 Hz for smoother real-time sensor response
LOG_DIR = "logs"
DEFAULT_TOL = 2.0
GRACE_SECONDS = 0.0   # default 0 so crossings count immediately; adjust via CLI if needed

class PulseCounter:
    def __init__(self, keep_seconds=10.0):
        self.keep_seconds = keep_seconds
        self.lock = threading.Lock()
        self.deque = deque()
    def add(self, t=None):
        if t is None: t = time.monotonic()
        with self.lock:
            self.deque.append(t)
            cutoff = t - self.keep_seconds
            while self.deque and self.deque[0] < cutoff:
                self.deque.popleft()
    def count_recent(self, window=1.0):
        now = time.monotonic()
        cutoff = now - window
        with self.lock:
            while self.deque and self.deque[0] < now - self.keep_seconds:
                self.deque.popleft()
            cnt = 0
            for ts in reversed(self.deque):
                if ts >= cutoff:
                    cnt += 1
                else:
                    break
            return cnt

class DriveBackend:
    def __init__(self, df, tick_hz=TICK_HZ, debounce=0.0, circ=1.94, ppr=1.0,
                 gpio_pin=17, use_gpio=False, min_speed=0.0, debug=False, simulate_gpio=False):
        self.profile = df.copy()
        self.tick_hz = tick_hz
        self.dt = 1.0 / tick_hz
        self.debounce = float(debounce)
        self.circ = float(circ)
        self.ppr = float(ppr)
        self.gpio_pin = int(gpio_pin)
        self.use_gpio = bool(use_gpio) and (GPIO_AVAILABLE or simulate_gpio)
        self.simulate_gpio = bool(simulate_gpio) and not GPIO_AVAILABLE
        self.min_speed = float(min_speed)   # ignore tiny lower-crossings when actual < this
        self.debug = bool(debug)

        # runtime state
        self.running = False
        self.start_monotonic = None
        self.elapsed = 0.0

        # count and crossing flags
        self.violations = 0
        self.prev_inside = True
        self.last_cross_monotonic = None
        
        # CMVR/AIS compliant violation timing (0.20 second rule)
        self.violation_timer = 0.0
        self.last_violation_check = None
        self.cmvr_threshold = 0.20  # 200ms sustained violation required

        # speed source - SENSOR ONLY MODE
        self.mode = "real"  # Always use real sensor mode
        self.actual_speed = 0.0

        # pulse counting for real sensor mode
        self.pulse_counter = PulseCounter(keep_seconds=max(10, int(self.tick_hz*5)))

        # websockets clients
        self.clients = set()

        # profile arrays
        self.times = self.profile['time'].astype(float).values
        self.targets = self.profile['target'].astype(float).values
        self.uppers = self.profile['upper'].astype(float).values
        self.lowers = self.profile['lower'].astype(float).values
        self.profile_end = float(self.times[-1]) if len(self.times) else 0.0

        # logging
        os.makedirs(LOG_DIR, exist_ok=True)
        self.logfile = None
        self.csv_writer = None

        if self.use_gpio:
            self._setup_gpio()

    def _setup_gpio(self):
        if self.simulate_gpio:
            # Simulation mode for testing on Windows/non-Pi systems
            if self.debug:
                print(f"[GPIO] SIMULATION MODE - GPIO pin {self.gpio_pin} configured for testing")
                print("[GPIO] Real mode will work but use manual speed input until on Raspberry Pi")
            return
            
        try:
            GPIO.setmode(GPIO.BCM)
            # Setup for NPN proximity sensor with internal pull-up
            # NPN sensor: HIGH (idle/no detection), LOW (detected/pulled to ground)
            GPIO.setup(self.gpio_pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
            
            # Use both FALLING and RISING edge detection for NPN sensor
            # FALLING: sensor detects object (pulls to ground)
            # RISING: sensor loses object (releases to HIGH)
            GPIO.add_event_detect(self.gpio_pin, GPIO.BOTH, callback=self._gpio_cb, bouncetime=int(self.debounce*1000))
            
            # Initialize sensor state tracking
            self.last_gpio_state = GPIO.input(self.gpio_pin)
            self.gpio_active = False  # Track if sensor is currently detecting
            
            if self.debug:
                initial_state = "HIGH (idle)" if self.last_gpio_state else "LOW (detecting)"
                print(f"[GPIO] NPN SENSOR - BCM{self.gpio_pin} configured with pull-up resistor")
                print(f"[GPIO] Initial sensor state: {initial_state}")
                print(f"[GPIO] Debounce time: {self.debounce*1000:.0f}ms")
        except Exception as e:
            print("[GPIO] setup failed:", e)
            self.use_gpio = False

    def _gpio_cb(self, ch):
        current_time = time.monotonic()
        current_state = GPIO.input(ch)
        
        # NPN sensor logic: LOW = detecting, HIGH = idle
        if current_state != self.last_gpio_state:
            if current_state == GPIO.LOW:
                # Sensor detected object - count as pulse for speed calculation
                self.pulse_counter.add(current_time)
                self.gpio_active = True
                
                # Show pulse detection in terminal (always, not just debug mode)
                ts = time.strftime("%H:%M:%S.%f")[:-3]
                if hasattr(self, '_last_pulse_time'):
                    pulse_interval = current_time - self._last_pulse_time
                    freq = 1.0 / pulse_interval if pulse_interval > 0 else 0
                    rpm = freq * 60 / max(1.0, self.ppr)  # Calculate RPM
                    speed_estimate = (freq / self.ppr) * self.circ * 3.6  # Instant speed estimate
                    print(f"🔵 [{ts}] PULSE DETECTED -> Pin17=LOW | Interval={pulse_interval*1000:.1f}ms | Freq={freq:.2f}Hz | RPM={rpm:.1f} | Est.Speed={speed_estimate:.1f}km/h")
                else:
                    print(f"🔵 [{ts}] FIRST PULSE DETECTED -> NPN Sensor activated")
                self._last_pulse_time = current_time
                
                if self.debug:
                    print(f"[DEBUG] GPIO callback - Pin {ch} went LOW, pulse count incremented")
                    
            else:  # current_state == GPIO.HIGH
                # Sensor lost object
                self.gpio_active = False
                
                # Show sensor release in terminal
                ts = time.strftime("%H:%M:%S.%f")[:-3]
                print(f"⚪ [{ts}] SENSOR RELEASE -> Pin17=HIGH (object passed)")
                
                if self.debug:
                    print(f"[DEBUG] GPIO callback - Pin {ch} went HIGH, sensor idle")
            
            self.last_gpio_state = current_state

    def interp_profile(self, t):
        if t <= self.times[0]:
            return float(self.targets[0]), float(self.uppers[0]), float(self.lowers[0])
        if t >= self.times[-1]:
            return float(self.targets[-1]), float(self.uppers[-1]), float(self.lowers[-1])
        tg = float(np.interp(t, self.times, self.targets))
        up = float(np.interp(t, self.times, self.uppers))
        lo = float(np.interp(t, self.times, self.lowers))
        return tg, up, lo

    # websocket registration/handler
    async def register(self, ws):
        self.clients.add(ws)
        prof_msg = {"type":"profile","profile":{"time":self.times.tolist(),"target":self.targets.tolist(),"upper":self.uppers.tolist(),"lower":self.lowers.tolist()}}
        try:
            await ws.send(json.dumps(prof_msg))
            await ws.send(json.dumps(self.snapshot()))
            if self.debug:
                print("[WS] profile + snapshot sent")
        except Exception:
            pass

    def unregister(self, ws):
        self.clients.discard(ws)

    async def handler(self, websocket, path):
        await self.register(websocket)
        try:
            async for raw in websocket:
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                await self.handle_command(obj)
        except websockets.ConnectionClosed:
            pass
        finally:
            self.unregister(websocket)

    async def handle_command(self, obj):
        cmd = obj.get("cmd")
        if cmd == "start":
            if not self.running:
                self.running = True
                self.start_monotonic = time.monotonic() - self.elapsed
                # initialize prev_inside based on current sensor speed
                if self.use_gpio:
                    now_actual = self.compute_speed()
                else:
                    now_actual = 0.0  # No sensor available
                self.actual_speed = now_actual
                target, upper, lower = self.interp_profile(self.elapsed)
                self.prev_inside = (self.actual_speed >= lower and self.actual_speed <= upper)
                # Initialize CMVR/AIS violation timer
                self.violation_timer = 0.0
                self.last_violation_check = time.monotonic()
                
                # Always show test start information
                timestamp = time.strftime("%H:%M:%S")
                compliance_status = "✅ COMPLIANT" if self.prev_inside else "❌ NON-COMPLIANT"
                print(f"")
                print(f"🚀 [{timestamp}] DRIVE CYCLE TEST STARTED")
                print(f"   Initial Status: {compliance_status}")
                print(f"   Current Speed: {self.actual_speed:.1f} km/h")
                print(f"   Speed Limits: {lower:.1f} - {upper:.1f} km/h")
                print(f"   Profile Duration: {self.profile_end:.1f} seconds")
                print(f"   CMVR/AIS Threshold: {self.cmvr_threshold}s sustained violation")
                print(f"")
                print(f"📊 REAL-TIME MONITORING:")
                print(f"=" * 120)
                
                if self.debug:
                    print(f"[DEBUG] CMD start - elapsed={self.elapsed:.2f}s, GPIO={'enabled' if self.use_gpio else 'disabled'}")
                self._open_log()
        elif cmd == "stop":
            if self.running:
                self.running = False
                self._close_log()
                
                # Always show test stop information
                timestamp = time.strftime("%H:%M:%S")
                print(f"")
                print(f"=" * 120)
                print(f"⏹️  [{timestamp}] DRIVE CYCLE TEST STOPPED")
                print(f"   Total Time: {self.elapsed:.2f} seconds")
                print(f"   Final Speed: {self.actual_speed:.1f} km/h")
                print(f"   Total Violations: {self.violations}")
                print(f"   Test Status: {'❌ FAILED' if self.violations > 0 else '✅ PASSED'}")
                print(f"")
                
                if self.debug:
                    print("[DEBUG] CMD stop - test session ended")
        elif cmd == "reset":
            self.running = False
            self.elapsed = 0.0
            self.violations = 0
            self.prev_inside = True
            self.last_cross_monotonic = None
            # Reset CMVR/AIS violation timer
            self.violation_timer = 0.0
            self.last_violation_check = None
            self._close_log()
            await self.broadcast({"type":"reset"})
            if self.debug:
                print("[CMD] reset - CMVR/AIS timers cleared")
        elif cmd == "sensor_status":
            # Send current sensor status to frontend
            if self.use_gpio:
                current_sensor_state = "DETECTING" if hasattr(self, 'gpio_active') and self.gpio_active else "IDLE"
                gpio_level = "HIGH" if getattr(self, 'last_gpio_state', True) else "LOW"
                pulses_recent = self.pulse_counter.count_recent(1.0)
                
                status_msg = {
                    "type": "sensor_status",
                    "sensor_active": getattr(self, 'gpio_active', False),
                    "gpio_level": gpio_level,
                    "pulses_per_second": pulses_recent,
                    "current_speed": round(self.actual_speed, 2),
                    "gpio_pin": self.gpio_pin,
                    "sensor_type": "NPN Proximity"
                }
                await self.broadcast(status_msg)
                
                if self.debug:
                    print(f"[SENSOR-STATUS] State: {current_sensor_state}, GPIO: {gpio_level}, PPS: {pulses_recent}, Speed: {self.actual_speed:.1f} km/h")

    # logging
    def _open_log(self):
        try:
            ts = datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            fname = f"test_{ts}.csv"
            self.logfile = open(os.path.join(LOG_DIR, fname), "w", newline='')
            self.csv_writer = csv.writer(self.logfile)
            self.csv_writer.writerow(["iso","epoch_ms","time_s","target","upper","lower","actual","violations"])
            if self.debug: print(f"[LOG] opened {fname}")
        except Exception as e:
            print("[LOG] open failed:", e)
            self.logfile = None
            self.csv_writer = None

    def _write_log_row(self, elapsed, target, upper, lower, actual, violations):
        if not self.csv_writer: return
        try:
            self.csv_writer.writerow([datetime.now(dt.timezone.utc).isoformat(), int(time.time()*1000), round(elapsed,3), round(target,3), round(upper,3), round(lower,3), round(actual,3), int(violations)])
            self.logfile.flush()
        except Exception:
            pass

    def _close_log(self):
        if self.logfile:
            try: self.logfile.close()
            except: pass
        self.logfile = None
        self.csv_writer = None

    # compute speed from NPN sensor pulses (optimized for real-time response)
    def compute_speed(self):
        # Use adaptive window based on expected speed range
        window = 1.0  # 1 second window for better accuracy with NPN sensor
        pulses = self.pulse_counter.count_recent(window)
        
        # Convert pulses to speed
        pps = pulses / window  # pulses per second
        rps = pps / max(1.0, self.ppr)  # revolutions per second
        speed_mps = rps * self.circ  # meters per second
        speed_kmh = speed_mps * 3.6  # kilometers per hour
        
        # Apply smoothing but maintain responsiveness for NPN sensor
        if hasattr(self, '_prev_computed_speed'):
            # Use different smoothing factor based on speed change magnitude
            speed_diff = abs(speed_kmh - self._prev_computed_speed)
            if speed_diff > 5.0:  # Large change - less smoothing for responsiveness
                alpha = 0.8  # 80% new reading, 20% previous
            else:  # Small change - more smoothing for stability
                alpha = 0.6  # 60% new reading, 40% previous
            speed_kmh = alpha * speed_kmh + (1 - alpha) * self._prev_computed_speed
        
        self._prev_computed_speed = speed_kmh
        
        # Debug output for NPN sensor
        if self.debug and pulses > 0:
            print(f"[SPEED] NPN Sensor: {pulses} pulses in {window}s -> {speed_kmh:.1f} km/h (PPS: {pps:.2f}, RPS: {rps:.2f})")
        
        return speed_kmh

    async def broadcast(self, obj):
        if not self.clients: return
        data = json.dumps(obj)
        coros = []
        for ws in list(self.clients):
            try:
                coros.append(ws.send(data))
            except Exception:
                self.unregister(ws)
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    def snapshot(self):
        return {"type":"update","time":round(self.elapsed,2),"target":None,"upper":None,"lower":None,"actual":round(self.actual_speed,2),"violations":int(self.violations),"running":bool(self.running)}

    async def run_loop(self):
        if self.debug:
            print(f"[BACKEND] loop {self.tick_hz}Hz profile_end={self.profile_end}s GPIO={'on' if self.use_gpio else 'off'} debounce={self.debounce}s")
        while True:
            t0 = time.monotonic()
            if self.running:
                self.elapsed = time.monotonic() - (self.start_monotonic or time.monotonic())
                if self.elapsed < 0: self.elapsed = 0.0

                # SENSOR-ONLY MODE: Always use real NPN sensor input
                if self.use_gpio:
                    # Real NPN sensor mode - compute speed from GPIO pin 17 pulses
                    prev_speed = getattr(self, '_prev_sensor_speed', 0.0)
                    self.actual_speed = self.compute_speed()
                    self._prev_sensor_speed = self.actual_speed
                    
                    # REAL-TIME TERMINAL DISPLAY: Show sensor values continuously
                    pulses_recent = self.pulse_counter.count_recent(1.0)
                    gpio_level = "HIGH" if getattr(self, 'last_gpio_state', True) else "LOW"
                    sensor_status = "DETECTING" if hasattr(self, 'gpio_active') and self.gpio_active else "IDLE"
                    
                    # Show real-time values every tick when running (10Hz updates)
                    if self.running:
                        timestamp = time.strftime("%H:%M:%S")
                        target, upper, lower = self.interp_profile(self.elapsed)
                        compliance = "✓ COMPLIANT" if (self.actual_speed >= lower and self.actual_speed <= upper) else "✗ VIOLATION"
                        
                        print(f"[{timestamp}] SENSOR: Pin17={gpio_level} | Status={sensor_status:>9} | PPS={pulses_recent:>2} | "
                              f"Speed={self.actual_speed:>5.1f}km/h | Target={target:>5.1f} | Limits=[{lower:>4.1f}-{upper:>4.1f}] | {compliance} | "
                              f"Violations={self.violations} | Timer={self.violation_timer:.3f}s")
                    
                    # Enhanced debug output for NPN sensor (only when significant changes occur)
                    if self.debug:
                        if abs(self.actual_speed - prev_speed) > 0.5:
                            print(f"[NPN-SENSOR] Speed change detected: {prev_speed:.1f} -> {self.actual_speed:.1f} km/h (Δ{self.actual_speed - prev_speed:+.1f})")
                        
                        # Periodic detailed sensor status (every 5 seconds for comprehensive monitoring)
                        if not hasattr(self, '_last_sensor_log') or time.monotonic() - self._last_sensor_log > 5.0:
                            self._last_sensor_log = time.monotonic()
                            total_pulses = len(self.pulse_counter.deque) if hasattr(self.pulse_counter, 'deque') else 0
                            print(f"[DEBUG] NPN Sensor Details: GPIO{self.gpio_pin}={gpio_level}, Pulses/sec={pulses_recent}, "
                                  f"Total pulses={total_pulses}, Circumference={self.circ}m, Speed={self.actual_speed:.2f} km/h")
                        
                else:
                    # GPIO not available - show error and use zero speed
                    self.actual_speed = 0.0
                    if self.debug:
                        if not hasattr(self, '_last_error_log') or time.monotonic() - self._last_error_log > 10.0:
                            self._last_error_log = time.monotonic()
                            print(f"[ERROR] GPIO not enabled! NPN sensor required. Use --use-gpio flag and run with sudo on Raspberry Pi")

                target, upper, lower = self.interp_profile(self.elapsed)

                inside = (self.actual_speed >= lower) and (self.actual_speed <= upper)
                crossed_event = (not inside) and self.prev_inside
                nowm = time.monotonic()
                crossed_flag = False
                cross_side = None

                # CMVR/AIS COMPLIANT VIOLATION DETECTION (0.20 second rule)
                # Calculate delta time for violation timer
                current_time = nowm
                if self.last_violation_check is None:
                    self.last_violation_check = current_time
                    delta_time = 0.0
                else:
                    delta_time = current_time - self.last_violation_check
                    self.last_violation_check = current_time
                
                # Check if speed is outside limits (actual speed > speed limit + tolerance)
                if not inside:  # Speed is outside valid range
                    # Increment violation timer
                    self.violation_timer += delta_time
                    
                    # Determine which boundary is violated
                    if self.actual_speed > upper:
                        cross_side = "upper"
                    else:
                        cross_side = "lower"
                    
                    # Check if violation timer meets CMVR/AIS threshold (0.20 seconds)
                    if self.violation_timer >= self.cmvr_threshold:
                        # Count violation and reset timer
                        self.violations += 1
                        crossed_flag = True
                        self.violation_timer = 0.0  # Reset timer after counting
                        
                        # ALWAYS show violations in terminal (not just debug mode)
                        violation_type = "ABOVE LIMIT (TOO FAST)" if cross_side == "upper" else "BELOW LIMIT (TOO SLOW)"
                        timestamp = time.strftime("%H:%M:%S")
                        print(f"")
                        print(f"🚨 [{timestamp}] VIOLATION #{self.violations} DETECTED! 🚨")
                        print(f"   Type: {violation_type}")
                        print(f"   Time: {self.elapsed:.2f}s (sustained for {self.cmvr_threshold}s)")
                        print(f"   Speed: {self.actual_speed:.1f} km/h | Limit: {lower:.1f}-{upper:.1f} km/h")
                        print(f"   Exceeded by: {abs(self.actual_speed - (upper if cross_side == 'upper' else lower)):.1f} km/h")
                        print(f"")
                        
                    elif self.violation_timer > 0:
                        # Show violation timer building up (every 0.05s for real-time feedback)
                        if not hasattr(self, '_last_timer_display') or time.monotonic() - self._last_timer_display > 0.05:
                            self._last_timer_display = time.monotonic()
                            progress_bar = "█" * int(self.violation_timer / self.cmvr_threshold * 20)
                            remaining_bar = "░" * (20 - len(progress_bar))
                            violation_type = "TOO FAST" if cross_side == "upper" else "TOO SLOW"
                            print(f"⚠️  VIOLATION BUILDING: [{progress_bar}{remaining_bar}] {self.violation_timer:.3f}s/{self.cmvr_threshold}s - {violation_type} ({self.actual_speed:.1f} km/h)")
                        
                else:  # Speed is within valid range
                    # Reset violation timer when speed returns to compliant range
                    if self.violation_timer > 0:
                        timestamp = time.strftime("%H:%M:%S")
                        print(f"✅ [{timestamp}] Speed returned to compliant range - timer reset (was {self.violation_timer:.3f}s)")
                    self.violation_timer = 0.0
                
                # Update state for next iteration
                self.prev_inside = inside

                # prepare message with CMVR/AIS compliance data
                msg = {
                    "type":"update",
                    "time": round(self.elapsed, 2),
                    "target": round(target, 2),
                    "upper": round(upper, 2),
                    "lower": round(lower, 2),
                    "actual": round(self.actual_speed, 2),
                    "violations": int(self.violations),
                    "running": True,
                    "crossed": bool(crossed_flag),
                    "cross_side": cross_side if crossed_flag else None,
                    "cmvr_timer": round(self.violation_timer, 3),
                    "cmvr_compliant": inside
                }

                self._write_log_row(self.elapsed, target, upper, lower, self.actual_speed, self.violations)
                await self.broadcast(msg)

                # stop at profile end
                if self.elapsed >= self.profile_end:
                    await self.broadcast({"type":"complete","time":round(self.elapsed,2),"violations":int(self.violations)})
                    self.running = False
                    self._close_log()

            t1 = time.monotonic()
            elapsed = t1 - t0
            await asyncio.sleep(max(0.0, self.dt - elapsed))

# main
async def main(profile_path, host='0.0.0.0', port=PORT, tol=DEFAULT_TOL, rebase=False, debug=False,
               gpio_pin=17, circ=1.94, ppr=1.0, use_gpio=False, debounce=0.0, min_speed=0.0, simulate_gpio=False):
    df = pd.read_csv(profile_path)
    if 'time' not in df.columns or 'target' not in df.columns:
        print("Profile CSV must contain 'time' and 'target' columns")
        sys.exit(1)
    if 'upper' not in df.columns:
        df['upper'] = df['target'] + tol
    if 'lower' not in df.columns:
        df['lower'] = df['target'] - tol
    df = df[['time','target','upper','lower']].sort_values('time').reset_index(drop=True)

    if rebase:
        t0 = df['time'].iloc[0]
        df['time'] = df['time'] - t0
        if debug:
            print(f"[MAIN] rebased by {t0}s -> new start {df['time'].iloc[0]}s")

    backend = DriveBackend(df, tick_hz=TICK_HZ, debounce=debounce, circ=circ, ppr=ppr,
                           gpio_pin=gpio_pin, use_gpio=use_gpio, min_speed=min_speed, debug=debug, simulate_gpio=simulate_gpio)

    try:
        server = await websockets.serve(lambda ws, path=None: backend.handler(ws, path), host, port)
    except OSError as e:
        print("Fatal: could not bind websocket port:", e)
        raise

    gpio_status = "ON" if backend.use_gpio else "OFF"
    gpio_pin_info = f" (Pin {backend.gpio_pin})" if backend.use_gpio else ""
    print(f"[MAIN] WebSocket server ws://{host}:{port}  GPIO={gpio_status}{gpio_pin_info}")
    
    if backend.use_gpio:
        print(f"[GPIO] NPN SENSOR MODE ENABLED - Pin {backend.gpio_pin} ready for pulse detection")
        print(f"[GPIO] Wheel circumference: {backend.circ}m, Pulses per revolution: {backend.ppr}")
        print(f"[GPIO] Sensor wiring: Red->12V, Black->GND, Green->GPIO{backend.gpio_pin}")
        print(f"")
        print(f"📡 REAL-TIME SENSOR MONITORING ACTIVE")
        print(f"   Terminal will display live sensor values when test is running")
        print(f"   Format: [TIME] SENSOR: Pin17=STATE | Status=DETECTING/IDLE | PPS=X | Speed=X.Xkm/h | Target=X.X | Limits=[X.X-X.X] | COMPLIANT/VIOLATION")
        print(f"   Violations will be highlighted with 🚨 alerts")
        print(f"   Press Ctrl+C to stop")
        print(f"")
    else:
        print("[ERROR] GPIO REQUIRED! This system requires NPN sensor input.")
        print("[ERROR] Run with: sudo python3 Backend.py --profile drive_cycles.csv --use-gpio --gpio-pin 17 --circ 1.94 --debug")
        sys.exit(1)
    
    loop = asyncio.get_running_loop()
    loop.create_task(backend.run_loop())

    stop = asyncio.Future()
    def _on_signal():
        if not stop.done(): stop.set_result(None)
    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal)
        loop.add_signal_handler(signal.SIGTERM, _on_signal)
    except Exception:
        pass

    try:
        await stop
    finally:
        server.close()
        await server.wait_closed()
        backend._close_log()
        if backend.use_gpio:
            try:
                GPIO.remove_event_detect(backend.gpio_pin)
                GPIO.cleanup()
            except Exception:
                pass
        print("[MAIN] shutdown complete")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', required=True)
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=PORT)
    parser.add_argument('--tol', type=float, default=DEFAULT_TOL)
    parser.add_argument('--rebase', action='store_true')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--gpio-pin', type=int, default=17)
    parser.add_argument('--circ', type=float, default=1.94)
    parser.add_argument('--ppr', type=float, default=1.0)
    parser.add_argument('--use-gpio', action='store_true')
    parser.add_argument('--simulate-gpio', action='store_true', help='simulate GPIO for testing on non-Pi systems')
    parser.add_argument('--debounce', type=float, default=0.0)
    parser.add_argument('--min-speed', type=float, default=0.0, help='ignore lower crossings below this actual speed (km/h)')
    args = parser.parse_args()

    print("[MAIN] backend starting with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol, rebase=args.rebase,
                         debug=args.debug, gpio_pin=args.gpio_pin, circ=args.circ, ppr=args.ppr,
                         use_gpio=args.use_gpio, debounce=args.debounce, min_speed=args.min_speed, 
                         simulate_gpio=args.simulate_gpio))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Fatal:", e)
        raise
