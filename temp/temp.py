#!/usr/bin/env python3
"""
backend.py - Drive cycle backend (Option C: count a violation for each inside->outside crossing)

This version uses the exact same GPIO usage pattern as your working npn_test_gpio.py:
 - direct import RPi.GPIO at top
 - GPIO.setmode(GPIO.BCM)
 - GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

Usage:
  # Manual (no GPIO)
  python3 backend.py --profile drive_cycles.csv --rebase --debug

  # With GPIO on Pi (use system python with sudo)
  sudo /usr/bin/python3 backend.py --profile drive_cycles.csv --use-gpio --gpio-pin 17 --circ 1.94 --ppr 1 --debounce 0.05 --debug

Notes:
 - Logs written in logs/test_<timestamp>.csv at ~1Hz
 - Each inside->outside crossing increments violations (small CROSS_DEBOUNCE to avoid bounce double-counts)
 - At profile end the code will save a PNG plot if matplotlib is installed
"""

import argparse
import asyncio
import csv
import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime
import datetime as dt
import threading

import numpy as np
import pandas as pd
import websockets

# Import RPi.GPIO directly — same as your working script
try:
    import RPi.GPIO as GPIO
    RPI_GPIO_AVAILABLE = True
except Exception as e:
    GPIO = None
    RPI_GPIO_AVAILABLE = False

# Optional plotting
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    MATPLOTLIB_PRESENT = True
except Exception:
    MATPLOTLIB_PRESENT = False

# Defaults
PORT = 8765
TICK_HZ = 10
LOG_DIR = "logs"
DEFAULT_TOL = 2.0
CROSS_DEBOUNCE = 0.20   # seconds to debounce cross counting
LOG_INTERVAL_S = 1.0    # write logfile ~1Hz

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
    def __init__(self, df, tick_hz=TICK_HZ, circ=1.94, ppr=1.0,
                 gpio_pin=17, use_gpio=False, debounce=0.05, debug=False, simulate_gpio=False):
        self.profile = df.copy()
        self.tick_hz = tick_hz
        self.dt = 1.0 / tick_hz
        self.circ = float(circ)
        self.ppr = float(ppr)
        self.gpio_pin = int(gpio_pin)
        self.request_use_gpio = bool(use_gpio)
        self.simulate_gpio = bool(simulate_gpio)
        self.use_gpio = False
        self.debounce = float(debounce)
        self.debug = bool(debug)

        # runtime
        self.running = False
        self.start_monotonic = None
        self.elapsed = 0.0

        # violation
        self.violations = 0
        self.prev_inside = True
        self._last_cross_time = 0.0

        # speeds
        self.mode = "manual"
        self.manual_speed = 0.0
        self.actual_speed = 0.0

        # pulse counting
        self.pulse_counter = PulseCounter(keep_seconds=max(10, int(self.tick_hz * 5)))

        # websockets
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
        self._last_log_time = -1.0
        self.log_rows_for_plot = []

        # set up GPIO if requested
        if self.request_use_gpio:
            self._setup_gpio()

    def _setup_gpio(self):
        # If user asked for simulate_gpio, don't touch real GPIO
        if self.simulate_gpio:
            self.use_gpio = True
            if self.debug:
                print("[GPIO] simulate mode enabled")
            return

        if not RPI_GPIO_AVAILABLE:
            print("[GPIO] RPi.GPIO import failed. Run on Raspberry Pi with python3-rpi.gpio installed and run with sudo.")
            self.use_gpio = False
            return

        # Match your working test: PUD_UP and direct setup
        try:
            GPIO.setmode(GPIO.BCM)
            GPIO.setup(self.gpio_pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

            bouncetime_ms = max(1, int(round(self.debounce * 1000))) if self.debounce > 0 else 50
            GPIO.add_event_detect(self.gpio_pin, GPIO.FALLING, callback=self._gpio_cb, bouncetime=bouncetime_ms)
            self.use_gpio = True
            if self.debug:
                print(f"[GPIO] BCM{self.gpio_pin} configured PUD_UP bouncetime={bouncetime_ms}ms")
        except Exception as e:
            print("[GPIO] setup failed:", e)
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
                print("[WS] sent profile + snapshot")
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
                if (self.mode == "manual") or (not self.use_gpio):
                    now_actual = float(self.manual_speed)
                else:
                    now_actual = self.compute_speed()
                self.actual_speed = now_actual
                target, upper, lower = self.interp_profile(self.elapsed)
                self.prev_inside = (self.actual_speed >= lower and self.actual_speed <= upper)
                if self.debug:
                    print(f"[CMD] start elapsed={self.elapsed:.2f} initial_speed={self.actual_speed:.2f} prev_inside={self.prev_inside}")
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
            self._last_cross_time = 0.0
            self._close_log()
            await self.broadcast({"type":"reset"})
            if self.debug:
                print("[CMD] reset")
        elif cmd == "set_mode":
            m = obj.get("mode")
            if m in ("manual","real"):
                self.mode = m
                if self.debug:
                    print(f"[CMD] set_mode {m} (gpio_enabled={self.use_gpio})")
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
            self._last_log_time = -1.0
            self.log_rows_for_plot = []
            if self.debug:
                print(f"[LOG] opened {fname}")
        except Exception as e:
            print("[LOG] open failed:", e)
            self.logfile = None
            self.csv_writer = None

    def _write_log_row(self, elapsed, target, upper, lower, actual, violations):
        now = time.monotonic()
        if self._last_log_time < 0 or (now - self._last_log_time) >= LOG_INTERVAL_S:
            self._last_log_time = now
            if not self.csv_writer:
                return
            try:
                self.csv_writer.writerow([datetime.now(dt.timezone.utc).isoformat(), int(time.time()*1000), round(elapsed,3), round(target,3), round(upper,3), round(lower,3), round(actual,3), int(violations)])
                self.logfile.flush()
                self.log_rows_for_plot.append((elapsed, target, upper, lower, actual))
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
        window = 0.5
        pulses = self.pulse_counter.count_recent(window)
        pps = pulses / window
        rps = pps / max(1.0, self.ppr)
        speed_mps = rps * self.circ
        speed_kmh = speed_mps * 3.6
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

    def _save_final_plot(self):
        if not MATPLOTLIB_PRESENT:
            if self.debug:
                print("[PLOT] matplotlib not installed; skipping PNG save")
            return
        if not self.log_rows_for_plot:
            if self.debug:
                print("[PLOT] no log rows to plot")
            return
        try:
            arr = np.array(self.log_rows_for_plot)
            times = arr[:,0].astype(float)
            targets = arr[:,1].astype(float)
            uppers = arr[:,2].astype(float)
            lowers = arr[:,3].astype(float)
            actuals = arr[:,4].astype(float)
            ts = datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            out_png = os.path.join(LOG_DIR, f"drive_plot_{ts}.png")
            plt.figure(figsize=(12,5))
            plt.plot(times, uppers, label='Upper', linewidth=1.5)
            plt.plot(times, lowers, label='Lower', linewidth=1.5)
            plt.plot(times, targets, label='Target', linewidth=1.5)
            plt.plot(times, actuals, label='Actual', linewidth=1.8)
            plt.xlabel('Time (s)')
            plt.ylabel('Speed (km/h)')
            plt.legend()
            plt.grid(True, alpha=0.25)
            plt.tight_layout()
            plt.savefig(out_png, dpi=150)
            plt.close()
            print(f"[PLOT] Saved final PNG: {out_png}")
        except Exception as e:
            print("[PLOT] Save failed:", e)

    async def run_loop(self):
        if self.debug:
            print(f"[BACKEND] running {self.tick_hz}Hz profile_end={self.profile_end}s GPIO={'on' if self.use_gpio else 'off'} debounce={self.debounce}s")
        while True:
            t0 = time.monotonic()
            if self.running:
                self.elapsed = time.monotonic() - (self.start_monotonic or time.monotonic())
                if self.elapsed < 0:
                    self.elapsed = 0.0

                # source selection
                if self.mode == "manual":
                    self.actual_speed = float(self.manual_speed)
                elif self.mode == "real" and self.use_gpio:
                    prev_speed = getattr(self, '_prev_sensor_speed', 0.0)
                    self.actual_speed = self.compute_speed()
                    self._prev_sensor_speed = self.actual_speed
                    if self.debug and abs(self.actual_speed - prev_speed) > 0.5:
                        print(f"[SENSOR] speed {self.actual_speed:.1f} km/h")
                else:
                    self.actual_speed = float(self.manual_speed)

                target, upper, lower = self.interp_profile(self.elapsed)
                inside = (self.actual_speed >= lower) and (self.actual_speed <= upper)

                # Option C: count violation each inside->outside transition
                crossed_flag = False
                cross_side = None
                if (not inside) and self.prev_inside:
                    nowm = time.monotonic()
                    if nowm - self._last_cross_time >= CROSS_DEBOUNCE:
                        self.violations += 1
                        crossed_flag = True
                        cross_side = "upper" if self.actual_speed > upper else "lower"
                        self._last_cross_time = nowm
                        if self.debug:
                            print(f"[VIOL] #{self.violations} at {self.elapsed:.2f}s actual={self.actual_speed:.2f} crossed {cross_side} band({lower:.2f}-{upper:.2f})")
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
                    "cross_side": cross_side if crossed_flag else None
                }

                self._write_log_row(self.elapsed, target, upper, lower, self.actual_speed, self.violations)
                await self.broadcast(msg)

                if self.elapsed >= self.profile_end:
                    await self.broadcast({"type":"complete","time":round(self.elapsed,2),"violations":int(self.violations)})
                    self._save_final_plot()
                    self.running = False
                    self._close_log()

            t1 = time.monotonic()
            elapsed = t1 - t0
            await asyncio.sleep(max(0.0, self.dt - elapsed))

# main
async def main(profile_path, host='0.0.0.0', port=PORT, tol=DEFAULT_TOL, rebase=False, debug=False,
               gpio_pin=17, circ=1.94, ppr=1.0, use_gpio=False, debounce=0.05, simulate_gpio=False):
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
            print(f"[MAIN] rebased by {t0}s -> start {df['time'].iloc[0]}s")

    backend = DriveBackend(df, tick_hz=TICK_HZ, circ=circ, ppr=ppr,
                           gpio_pin=gpio_pin, use_gpio=use_gpio, debounce=debounce, debug=debug, simulate_gpio=simulate_gpio)

    try:
        server = await websockets.serve(lambda ws, path: backend.handler(ws, path), host, port)
    except Exception as e:
        print("[MAIN] WebSocket bind failed:", e)
        raise

    gpio_status = "ON" if backend.use_gpio else "OFF"
    gpio_info = f" (Pin {backend.gpio_pin})" if backend.use_gpio else ""
    print(f"[MAIN] WebSocket server ws://{host}:{port}  GPIO={gpio_status}{gpio_info}")
    if backend.use_gpio:
        print(f"[GPIO] Real mode enabled on BCM{backend.gpio_pin} circ={backend.circ}m ppr={backend.ppr}")
    else:
        if backend.request_use_gpio:
            print("[WARN] GPIO requested but not available - running manual mode")
        else:
            print("[INFO] Manual mode (no hardware pulses)")

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
    parser.add_argument('--simulate-gpio', action='store_true')
    parser.add_argument('--debounce', type=float, default=0.05, help='GPIO debounce seconds')
    args = parser.parse_args()

    print("[MAIN] backend starting with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol, rebase=args.rebase,
                         debug=args.debug, gpio_pin=args.gpio_pin, circ=args.circ, ppr=args.ppr,
                         use_gpio=args.use_gpio, debounce=args.debounce, simulate_gpio=args.simulate_gpio))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Fatal:", e)
        raise
