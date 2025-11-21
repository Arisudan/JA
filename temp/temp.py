#!/usr/bin/env python3
"""
backend.py - Drive cycle backend with GPIO pulse input and WebSocket frontend

Features:
 - Read drive profile CSV (time, target) and compute upper/lower bands (tol)
 - Serve profile + live updates over WebSocket (port 8765)
 - Manual mode (slider) and Real mode (GPIO pulses -> speed)
 - Pulse counting using ISR callback and thread-safe deque
 - CMVR-style violation logic (sustained out-of-band >= 0.20s increments violations)
 - CSV logging of runtime data in logs/
 - Dynamic import of RPi.GPIO; supports --simulate-gpio for development

Usage (manual):
  python3 backend.py --profile drive_cycles.csv --rebase --debug

Usage (GPIO on Raspberry Pi):
  sudo python3 backend.py --profile drive_cycles.csv --use-gpio --gpio-pin 17 --circ 1.94 --ppr 1 --debounce 0.05 --debug

Dependencies:
  pip3 install pandas numpy websockets
  (On Raspberry Pi) sudo apt install python3-rpi.gpio
"""

import argparse
import asyncio
import csv
import json
import os
import signal
import sys
import time
import importlib
from collections import deque
from datetime import datetime
import datetime as dt
import threading

import numpy as np
import pandas as pd
import websockets

# Globals for optional GPIO module (imported lazily)
GPIO = None
GPIO_AVAILABLE = False

# Defaults
PORT = 8765
TICK_HZ = 10
LOG_DIR = "logs"
DEFAULT_TOL = 2.0
GRACE_SECONDS = 0.0

class PulseCounter:
    def __init__(self, keep_seconds=10.0):
        self.keep_seconds = keep_seconds
        self.lock = threading.Lock()
        self.deque = deque()
    def add(self, t=None):
        if t is None:
            t = time.monotonic()
        with self.lock:
            self.deque.append(t)
            cutoff = t - self.keep_seconds
            while self.deque and self.deque[0] < cutoff:
                self.deque.popleft()
    def count_recent(self, window=1.0):
        now = time.monotonic()
        cutoff = now - window
        with self.lock:
            # purge very old
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
        self.request_use_gpio = bool(use_gpio)
        self.simulate_gpio = bool(simulate_gpio)
        self.use_gpio = False
        self.min_speed = float(min_speed)
        self.debug = bool(debug)

        # runtime
        self.running = False
        self.start_monotonic = None
        self.elapsed = 0.0

        # violations
        self.violations = 0
        self.prev_inside = True
        self.last_cross_monotonic = None

        # CMVR timing
        self.violation_timer = 0.0
        self.last_violation_check = None
        self.cmvr_threshold = 0.20

        # speeds
        self.mode = "manual"
        self.manual_speed = 0.0
        self.actual_speed = 0.0

        # pulses
        self.pulse_counter = PulseCounter(keep_seconds=max(10, int(self.tick_hz * 5)))

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

        # try setup gpio if requested
        if self.request_use_gpio:
            self._setup_gpio()

    def _setup_gpio(self):
        """Lazy import and configure RPi.GPIO. Sets self.use_gpio True if available and configured."""
        global GPIO, GPIO_AVAILABLE
        if self.simulate_gpio:
            self.use_gpio = True
            if self.debug:
                print(f"[GPIO] SIMULATION MODE enabled for pin BCM{self.gpio_pin}")
            return

        try:
            GPIO = importlib.import_module('RPi.GPIO')
            GPIO_AVAILABLE = True
        except Exception as exc:
            GPIO_AVAILABLE = False
            print("[GPIO] RPi.GPIO import failed:", exc)
            print("[GPIO] To enable hardware mode install python3-rpi.gpio and run with sudo on a Raspberry Pi.")
            self.use_gpio = False
            return

        # configure
        try:
            GPIO.setmode(GPIO.BCM)
            # Assume external divider / opto used; disable internal pull-ups to avoid contention
            GPIO.setup(self.gpio_pin, GPIO.IN, pull_up_down=GPIO.PUD_OFF)

            # compute bouncetime in ms, ensure > 0
            if isinstance(self.debounce, (int, float)) and self.debounce > 0:
                bouncetime_ms = max(1, int(round(self.debounce * 1000)))
            else:
                bouncetime_ms = 5

            # Attach event detect
            try:
                GPIO.add_event_detect(self.gpio_pin, GPIO.FALLING, callback=self._gpio_cb, bouncetime=bouncetime_ms)
            except TypeError:
                # Some older RPi.GPIO expect named param bouncetime differently - still ensure positive int
                GPIO.add_event_detect(self.gpio_pin, GPIO.FALLING, callback=self._gpio_cb, bouncetime_ms)
            self.use_gpio = True
            if self.debug:
                print(f"[GPIO] Configured BCM{self.gpio_pin} (PUD_OFF), bouncetime={bouncetime_ms}ms")
        except Exception as e:
            print("[GPIO] setup failed:", e)
            print("[GPIO] Ensure you ran with sudo and the RPi.GPIO package is installed for this interpreter.")
            self.use_gpio = False

    def _gpio_cb(self, ch):
        ts = time.monotonic()
        self.pulse_counter.add(ts)
        if self.debug:
            if hasattr(self, '_last_pulse_time'):
                interval = ts - self._last_pulse_time
                freq = 1.0 / interval if interval > 0 else 0.0
                print(f"[GPIO{self.gpio_pin}] Pulse {ts:.3f} (interval {interval*1000:.1f} ms, {freq:.1f} Hz)")
            else:
                print(f"[GPIO{self.gpio_pin}] First pulse at {ts:.3f}")
            self._last_pulse_time = ts

    def interp_profile(self, t):
        if t <= self.times[0]:
            return float(self.targets[0]), float(self.uppers[0]), float(self.lowers[0])
        if t >= self.times[-1]:
            return float(self.targets[-1]), float(self.uppers[-1]), float(self.lowers[-1])
        tg = float(np.interp(t, self.times, self.targets))
        up = float(np.interp(t, self.times, self.uppers))
        lo = float(np.interp(t, self.times, self.lowers))
        return tg, up, lo

    async def register(self, ws):
        self.clients.add(ws)
        prof_msg = {"type":"profile","profile":{"time":self.times.tolist(),"target":self.targets.tolist(),"upper":self.uppers.tolist(),"lower":self.lowers.tolist()}}
        try:
            await ws.send(json.dumps(prof_msg))
            await ws.send(json.dumps(self.snapshot()))
            if self.debug:
                print("[WS] Sent profile + snapshot to client")
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
                # initialize prev_inside
                if (self.mode == "manual") or (not self.use_gpio):
                    now_actual = float(self.manual_speed)
                else:
                    now_actual = self.compute_speed()
                self.actual_speed = now_actual
                target, upper, lower = self.interp_profile(self.elapsed)
                self.prev_inside = (self.actual_speed >= lower and self.actual_speed <= upper)
                self.violation_timer = 0.0
                self.last_violation_check = time.monotonic()
                if self.debug:
                    print(f"[CMD] start (elapsed {self.elapsed:.2f}) initial_speed={self.actual_speed:.2f}, prev_inside={self.prev_inside}")
                self._open_log()
        elif cmd == "stop":
            if self.running:
                self.running = False
                self._close_log()
                if self.debug:
                    print("[CMD] stop")
        elif cmd == "reset":
            self.running = False
            self.elapsed = 0.0
            self.violations = 0
            self.prev_inside = True
            self.last_cross_monotonic = None
            self.violation_timer = 0.0
            self.last_violation_check = None
            self._close_log()
            await self.broadcast({"type":"reset"})
            if self.debug:
                print("[CMD] reset")
        elif cmd == "set_mode":
            m = obj.get("mode")
            if m in ("manual","real"):
                self.mode = m
                if self.debug:
                    print(f"[CMD] set_mode -> {m} (gpio_enabled={self.use_gpio})")
                await self.broadcast({"type":"mode_status","mode":m,"gpio_enabled":self.use_gpio,"gpio_pin": self.gpio_pin if self.use_gpio else None})
        elif cmd == "manual_speed":
            try:
                self.manual_speed = float(obj.get("speed", 0.0))
            except Exception:
                pass

    def _open_log(self):
        try:
            ts = datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            fname = f"test_{ts}.csv"
            self.logfile = open(os.path.join(LOG_DIR, fname), "w", newline='')
            self.csv_writer = csv.writer(self.logfile)
            self.csv_writer.writerow(["iso","epoch_ms","time_s","target","upper","lower","actual","violations"])
            if self.debug:
                print(f"[LOG] opened {fname}")
        except Exception as e:
            print("[LOG] open failed:", e)
            self.logfile = None
            self.csv_writer = None

    def _write_log_row(self, elapsed, target, upper, lower, actual, violations):
        if not self.csv_writer:
            return
        try:
            self.csv_writer.writerow([datetime.now(dt.timezone.utc).isoformat(), int(time.time()*1000), round(elapsed,3), round(target,3), round(upper,3), round(lower,3), round(actual,3), int(violations)])
            self.logfile.flush()
        except Exception:
            pass

    def _close_log(self):
        if self.logfile:
            try:
                self.logfile.close()
            except Exception:
                pass
        self.logfile = None
        self.csv_writer = None

    def compute_speed(self):
        # responsive window
        window = 0.5
        pulses = self.pulse_counter.count_recent(window)
        pps = pulses / window
        rps = pps / max(1.0, self.ppr)
        speed_mps = rps * self.circ
        speed_kmh = speed_mps * 3.6

        # smoothing
        if hasattr(self, '_prev_computed_speed'):
            speed_kmh = 0.7 * speed_kmh + 0.3 * self._prev_computed_speed
        self._prev_computed_speed = speed_kmh
        return speed_kmh

    async def broadcast(self, obj):
        if not self.clients:
            return
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
                if self.elapsed < 0:
                    self.elapsed = 0.0

                # choose speed source
                if self.mode == "manual":
                    self.actual_speed = float(self.manual_speed)
                elif self.mode == "real" and self.use_gpio:
                    prev_speed = getattr(self, '_prev_sensor_speed', 0.0)
                    self.actual_speed = self.compute_speed()
                    self._prev_sensor_speed = self.actual_speed
                    if self.debug and abs(self.actual_speed - prev_speed) > 0.5:
                        print(f"[SENSOR] speed {self.actual_speed:.1f} km/h (delta {self.actual_speed - prev_speed:+.1f})")
                else:
                    # requested real but GPIO not enabled -> fallback manual
                    self.actual_speed = float(self.manual_speed)

                target, upper, lower = self.interp_profile(self.elapsed)

                inside = (self.actual_speed >= lower) and (self.actual_speed <= upper)
                nowm = time.monotonic()
                crossed_flag = False
                cross_side = None

                # CMVR timing delta
                current_time = nowm
                if self.last_violation_check is None:
                    self.last_violation_check = current_time
                    delta_time = 0.0
                else:
                    delta_time = current_time - self.last_violation_check
                    self.last_violation_check = current_time

                if not inside:
                    self.violation_timer += delta_time
                    cross_side = "upper" if self.actual_speed > upper else "lower"

                    if self.violation_timer >= self.cmvr_threshold:
                        self.violations += 1
                        crossed_flag = True
                        self.violation_timer = 0.0
                        if self.debug:
                            typ = "ABOVE" if cross_side == "upper" else "BELOW"
                            print(f"[CMVR] VIOLATION #{self.violations} at {self.elapsed:.2f}s - {typ} limit (speed {self.actual_speed:.1f}, band {lower:.1f}-{upper:.1f})")
                    elif self.debug and self.violation_timer > 0:
                        print(f"[TIMER] building {self.violation_timer:.3f}s / {self.cmvr_threshold}s")
                else:
                    if self.violation_timer > 0 and self.debug:
                        print(f"[CMVR] timer reset (was {self.violation_timer:.3f}s)")
                    self.violation_timer = 0.0

                self.prev_inside = inside

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

                # stop at end
                if self.elapsed >= self.profile_end:
                    await self.broadcast({"type":"complete","time":round(self.elapsed,2),"violations":int(self.violations)})
                    self.running = False
                    self._close_log()

            t1 = time.monotonic()
            elapsed = t1 - t0
            await asyncio.sleep(max(0.0, self.dt - elapsed))

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

    # WebSocket server (handler bound to instance)
    try:
        server = await websockets.serve(lambda ws, path: backend.handler(ws, path), host, port)
    except OSError as e:
        print("Fatal: could not bind websocket port:", e)
        raise

    gpio_status = "ON" if backend.use_gpio else "OFF"
    gpio_info = f" (Pin {backend.gpio_pin})" if backend.use_gpio else ""
    print(f"[MAIN] WebSocket server ws://{host}:{port}  GPIO={gpio_status}{gpio_info}")

    if backend.use_gpio:
        print(f"[GPIO] Real sensor mode ENABLED on BCM{backend.gpio_pin} - circ={backend.circ}m ppr={backend.ppr}")
    else:
        if backend.request_use_gpio:
            print("[ERROR] GPIO requested but not available - running in manual mode")
        else:
            print("[GPIO] Manual mode (no hardware pulses)")

    loop = asyncio.get_running_loop()
    loop.create_task(backend.run_loop())

    stop = asyncio.Future()
    def _on_signal():
        if not stop.done():
            stop.set_result(None)
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
        if backend.use_gpio and GPIO is not None:
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
    parser.add_argument('--debounce', type=float, default=0.0, help='debounce seconds (float)')
    parser.add_argument('--min-speed', type=float, default=0.0, help='ignore lower crossings below this actual speed (km/h)')
    args = parser.parse_args()

    print("[MAIN] backend starting with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol, rebase=args.rebase,
                         debug=args.debug, gpio_pin=args.gpio_pin, circ=args.circ, ppr=args.ppr,
                         use_gpio=args.use_gpio, debounce=args.debounce, min_speed=args.min_speed, simulate_gpio=args.simulate_gpio))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Fatal:", e)
        raise
