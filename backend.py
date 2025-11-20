#!/usr/bin/env python3
"""
backend.py - Drive cycle websocket backend with real sensor input (1 pulse per rotation)

Usage (example):
  sudo python3 backend.py --profile drive_cycle.csv --rebase --use-gpio --gpio-pin 17 --circ 1.94 --min_violation 1.0 --debug

Dependencies:
  pip install pandas numpy websockets
Run with sudo on Raspberry Pi when using GPIO.
"""

import asyncio
import json
import argparse
import time
from datetime import datetime
import os
import csv
import signal
import sys
from collections import deque
import threading

import pandas as pd
import numpy as np
import websockets

# Try import RPi.GPIO; fall back if not present (simulation mode)
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except Exception:
    GPIO_AVAILABLE = False

# ----------------- Config defaults -----------------
PORT = 8765
TICK_HZ = 5
GRACE_SECONDS = 5.0
VIOLATION_MIN_DURATION = 1.0
DEFAULT_TOL = 2.0
LOG_DIR = "logs"
# ----------------------------------------------------

class PulseCounter:
    """Thread-safe deque of monotonic timestamps for recent pulses."""
    def __init__(self, keep_seconds=10.0):
        self.keep_seconds = keep_seconds
        self.deque = deque()
        self.lock = threading.Lock()

    def add_pulse(self, t=None):
        if t is None:
            t = time.monotonic()
        with self.lock:
            self.deque.append(t)
            cutoff = t - self.keep_seconds
            while self.deque and self.deque[0] < cutoff:
                self.deque.popleft()

    def count_last(self, window_seconds):
        now = time.monotonic()
        cutoff = now - window_seconds
        with self.lock:
            while self.deque and self.deque[0] < now - self.keep_seconds:
                self.deque.popleft()
            # count entries >= cutoff (iterate from right for early break)
            cnt = 0
            for ts in reversed(self.deque):
                if ts >= cutoff:
                    cnt += 1
                else:
                    break
            return cnt

    def last_timestamp(self):
        with self.lock:
            return self.deque[-1] if self.deque else None

class DriveBackend:
    def __init__(self, profile_df, tick_hz=TICK_HZ, grace=GRACE_SECONDS,
                 min_violation_duration=VIOLATION_MIN_DURATION, verbose=False,
                 pulses_per_rotation=1, wheel_circumference_m=1.94,
                 gpio_pin=17, use_gpio=False):
        self.profile = profile_df.copy()
        self.tick_hz = tick_hz
        self.dt = 1.0 / tick_hz
        self.grace = grace
        self.min_violation_duration = min_violation_duration
        self.verbose = verbose

        # runtime state
        self.running = False
        self.start_wall = None
        self.elapsed = 0.0

        # violation tracking
        self.violations = 0
        self._in_violation = False
        self._violation_timer_start = None
        self._violation_counted_for_event = False

        # speed source
        self.mode = "manual"
        self.manual_speed = 0.0
        self.actual_speed = 0.0

        # pulse counting
        self.ppr = float(pulses_per_rotation)
        self.circ = float(wheel_circumference_m)
        self.pulse_counter = PulseCounter(keep_seconds=max(10, int(tick_hz*5)))
        self.gpio_pin = int(gpio_pin)
        self.use_gpio = bool(use_gpio) and GPIO_AVAILABLE

        # websocket clients
        self.clients = set()

        # profile arrays
        self.times = self.profile['time'].values.astype(float)
        self.targets = self.profile['target'].values.astype(float)
        self.uppers = self.profile['upper'].values.astype(float)
        self.lowers = self.profile['lower'].values.astype(float)
        self.profile_end = float(self.times[-1]) if len(self.times) else 0.0

        # logging
        os.makedirs(LOG_DIR, exist_ok=True)
        self.logfile = None
        self.csv_writer = None
        self.current_test_id = None

        # GPIO setup
        if self.use_gpio:
            self._setup_gpio()

    def _setup_gpio(self):
        try:
            GPIO.setmode(GPIO.BCM)
            GPIO.setup(self.gpio_pin, GPIO.IN, pull_up_down=GPIO.PUD_OFF)
            # falling edge: sensor NPN pulls to GND on detection
            GPIO.add_event_detect(self.gpio_pin, GPIO.FALLING, callback=self._pulse_callback, bouncetime=10)
            if self.verbose:
                print(f"[GPIO] Listening on BCM{self.gpio_pin}")
        except Exception as e:
            print("[GPIO] Setup failed:", e)
            self.use_gpio = False

    def _pulse_callback(self, channel):
        # Called by RPi.GPIO in a separate thread
        self.pulse_counter.add_pulse()
        if self.verbose:
            print(f"[GPIO] pulse at {time.monotonic():.3f}")

    def interp_profile(self, t):
        if t <= self.times[0]:
            return float(self.targets[0]), float(self.uppers[0]), float(self.lowers[0])
        if t >= self.times[-1]:
            return float(self.targets[-1]), float(self.uppers[-1]), float(self.lowers[-1])
        target = float(np.interp(t, self.times, self.targets))
        upper = float(np.interp(t, self.times, self.uppers))
        lower = float(np.interp(t, self.times, self.lowers))
        return target, upper, lower

    # websocket registration
    async def register(self, websocket):
        self.clients.add(websocket)
        prof = {
            "type": "profile",
            "profile": {
                "time": self.times.tolist(),
                "target": self.targets.tolist(),
                "upper": self.uppers.tolist(),
                "lower": self.lowers.tolist()
            }
        }
        try:
            await websocket.send(json.dumps(prof))
            await websocket.send(json.dumps(self.snapshot()))
            if self.verbose:
                print("[WS] Sent profile snapshot")
        except Exception:
            pass

    def unregister(self, websocket):
        self.clients.discard(websocket)

    def snapshot(self):
        return {
            "type": "update",
            "time": round(self.elapsed, 2),
            "target": None,
            "upper": None,
            "lower": None,
            "actual": round(self.actual_speed, 2),
            "violations": int(self.violations),
            "running": bool(self.running)
        }

    async def broadcast(self, msg):
        if not self.clients:
            return
        data = json.dumps(msg)
        coros = []
        for ws in set(self.clients):
            try:
                coros.append(ws.send(data))
            except Exception:
                self.unregister(ws)
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    async def handler(self, websocket, path=None):
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
                self.start_wall = time.monotonic() - self.elapsed
                self._in_violation = False
                self._violation_timer_start = None
                self._violation_counted_for_event = False
                self._open_log()
                if self.verbose:
                    print("[CMD] Start")
        elif cmd == "stop":
            if self.running:
                self.running = False
                self._close_log()
                if self.verbose:
                    print("[CMD] Stop")
        elif cmd == "reset":
            self.running = False
            self.elapsed = 0.0
            self.violations = 0
            self._in_violation = False
            self._violation_timer_start = None
            self._violation_counted_for_event = False
            self._close_log()
            await self.broadcast({"type": "reset"})
            if self.verbose:
                print("[CMD] Reset")
        elif cmd == "set_mode":
            m = obj.get("mode")
            if m in ("manual", "real"):
                self.mode = m
                if self.verbose:
                    print("[CMD] Mode set to", m)
        elif cmd == "manual_speed":
            try:
                self.manual_speed = float(obj.get("speed", 0.0))
            except Exception:
                pass

    def _open_log(self):
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"test_{ts}.csv"
        path = os.path.join(LOG_DIR, fname)
        try:
            self.logfile = open(path, "w", newline='')
            self.csv_writer = csv.writer(self.logfile)
            self.csv_writer.writerow(["ts_iso", "epoch_ms", "time_s", "target_kmh", "upper_kmh", "lower_kmh", "actual_kmh", "violations"])
            self.logfile.flush()
            self.current_test_id = fname
        except Exception as e:
            if self.verbose:
                print("Failed to open log:", e)

    def _write_log_row(self, elapsed, target, upper, lower, actual, violations):
        if not self.csv_writer:
            return
        ts_iso = datetime.utcnow().isoformat()
        epoch_ms = int(time.time() * 1000)
        self.csv_writer.writerow([ts_iso, epoch_ms, round(elapsed,3), round(target,3), round(upper,3), round(lower,3), round(actual,3), int(violations)])
        try:
            self.logfile.flush()
        except:
            pass

    def _close_log(self):
        if self.logfile:
            try:
                self.logfile.close()
            except:
                pass
        self.logfile = None
        self.csv_writer = None
        self.current_test_id = None

    def compute_speed_from_pulses(self):
        """
        Compute speed (km/h) using pulses counted in last 1 second window.
        speed_kmh = (pulses_per_second / ppr) * circumference_m * 3.6
        """
        window = 1.0
        pulses = self.pulse_counter.count_last(window)
        pulses_per_second = pulses / window
        rotations_per_second = pulses_per_second / max(1.0, self.ppr)
        speed_mps = rotations_per_second * self.circ
        speed_kmh = speed_mps * 3.6
        return round(speed_kmh, 3)

    async def run_loop(self):
        print(f"[BACKEND] Loop {self.tick_hz}Hz. profile_end={self.profile_end}s grace={self.grace}s min_violation={self.min_violation_duration}s GPIO={'on' if self.use_gpio else 'off'} ppr={self.ppr} circ={self.circ}m")
        while True:
            t0 = time.monotonic()
            if self.running:
                self.elapsed = time.monotonic() - self.start_wall
                if self.elapsed < 0:
                    self.elapsed = 0.0

                # choose speed source
                if self.mode == "manual" or not self.use_gpio:
                    self.actual_speed = float(self.manual_speed)
                else:
                    self.actual_speed = self.compute_speed_from_pulses()

                target, upper, lower = self.interp_profile(self.elapsed)

                # violation detection
                out_of_range = (self.actual_speed < lower) or (self.actual_speed > upper)

                if self.elapsed <= self.grace:
                    self._in_violation = False
                    self._violation_timer_start = None
                    self._violation_counted_for_event = False
                else:
                    if out_of_range and not self._in_violation:
                        self._in_violation = True
                        self._violation_timer_start = time.monotonic()
                        self._violation_counted_for_event = False
                        if self.verbose:
                            print(f"[VIOL] started out-of-range at {self.elapsed:.2f}s actual={self.actual_speed} band=({lower},{upper})")
                    elif out_of_range and self._in_violation:
                        if (not self._violation_counted_for_event) and (time.monotonic() - (self._violation_timer_start or time.monotonic()) >= self.min_violation_duration):
                            self.violations += 1
                            self._violation_counted_for_event = True
                            if self.verbose:
                                print(f"[VIOL] counted #{self.violations} at {self.elapsed:.2f}s")
                    else:
                        if self._in_violation or self._violation_counted_for_event:
                            if self.verbose:
                                print(f"[VIOL] returned inside at {self.elapsed:.2f}s actual={self.actual_speed}")
                        self._in_violation = False
                        self._violation_timer_start = None
                        self._violation_counted_for_event = False

                msg = {
                    "type": "update",
                    "time": round(self.elapsed, 2),
                    "target": round(target, 2),
                    "upper": round(upper, 2),
                    "lower": round(lower, 2),
                    "actual": round(self.actual_speed, 2),
                    "violations": int(self.violations),
                    "running": True
                }

                self._write_log_row(self.elapsed, target, upper, lower, self.actual_speed, self.violations)
                await self.broadcast(msg)

                if self.elapsed >= self.profile_end:
                    await self.broadcast({"type": "complete", "time": round(self.elapsed, 2), "violations": int(self.violations)})
                    self.running = False
                    self._close_log()

            t1 = time.monotonic()
            elapsed = t1 - t0
            to_sleep = max(0.0, self.dt - elapsed)
            await asyncio.sleep(to_sleep)

# ---------------- main ----------------
async def main(profile_path, host="0.0.0.0", port=PORT, tol=DEFAULT_TOL, rebase=False,
               verbose=False, min_violation=VIOLATION_MIN_DURATION, gpio_pin=17, circ=1.94, ppr=1, use_gpio=False):
    df = pd.read_csv(profile_path)
    if 'time' not in df.columns or 'target' not in df.columns:
        raise SystemExit("Profile CSV must contain at least 'time' and 'target' columns.")
    if 'upper' not in df.columns:
        df['upper'] = df['target'] + tol
    if 'lower' not in df.columns:
        df['lower'] = df['target'] - tol
    df = df[['time', 'target', 'upper', 'lower']].sort_values('time').reset_index(drop=True)

    if rebase:
        t0v = df['time'].iloc[0]
        df['time'] = (df['time'] - t0v).astype(float)
        print(f"[MAIN] Rebased profile by subtracting {t0v} -> new start {df['time'].iloc[0]}s")

    backend = DriveBackend(df, tick_hz=TICK_HZ, grace=GRACE_SECONDS, min_violation_duration=min_violation,
                           verbose=verbose, pulses_per_rotation=ppr, wheel_circumference_m=circ,
                           gpio_pin=gpio_pin, use_gpio=use_gpio)

    server = await websockets.serve(lambda ws, path=None: backend.handler(ws, path), host, port)
    print(f"[MAIN] Websocket server running on ws://{host}:{port}   GPIO={'on' if backend.use_gpio else 'off'}")

    loop = asyncio.get_running_loop()
    loop.create_task(backend.run_loop())

    stop = asyncio.Future()
    def _on_signal(sig_name):
        if not stop.done():
            stop.set_result(None)
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: _on_signal("SIGINT"))
    except NotImplementedError:
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
        print("[MAIN] Shutdown complete.")

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--profile', required=True)
    p.add_argument('--host', default='0.0.0.0')
    p.add_argument('--port', type=int, default=PORT)
    p.add_argument('--tol', type=float, default=DEFAULT_TOL)
    p.add_argument('--rebase', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--min_violation', type=float, default=VIOLATION_MIN_DURATION)
    p.add_argument('--gpio-pin', type=int, default=17, help='BCM pin for sensor input')
    p.add_argument('--circ', type=float, default=1.94, help='wheel circumference (meters)')
    p.add_argument('--ppr', type=float, default=1.0, help='pulses per wheel rotation (1)')
    p.add_argument('--use-gpio', action='store_true', help='enable reading from Raspberry Pi GPIO')
    args = p.parse_args()

    print("[MAIN] Starting backend with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol, rebase=args.rebase,
                         verbose=args.debug, min_violation=args.min_violation, gpio_pin=args.gpio_pin,
                         circ=args.circ, ppr=args.ppr, use_gpio=args.use_gpio))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Fatal error:", e)
        raise
