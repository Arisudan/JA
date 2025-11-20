#!/usr/bin/env python3
"""
backend.py - Drive-cycle backend (full file)

Features:
 - Loads drive cycle CSV with columns: time, target (optional upper/lower).
 - If upper/lower not present they are computed as target +/- tol.
 - WebSocket server (ws://0.0.0.0:8765) serves profile and periodic updates.
 - Counts violations immediately when actual crosses from inside -> outside (upper or lower).
 - Debounce option to avoid double-counts from bouncing.
 - Emits "crossed": true in the update message on the tick when a crossing is counted.
 - Supports manual mode (slider) and real sensor GPIO mode (optional).
 - Logs per-test CSV rows to ./logs/
 - Use --debug for verbose prints.

Run example:
  # without GPIO
  python3 backend.py --profile drive_cycle.csv --rebase --debounce 0.5 --debug

  # with GPIO on Raspberry Pi (run with sudo)
  sudo python3 backend.py --profile drive_cycle.csv --rebase --use-gpio --gpio-pin 17 --circ 1.94 --debounce 0.5 --debug
"""

import asyncio
import argparse
import json
import time
import csv
import os
import signal
from collections import deque
from datetime import datetime
import threading

import pandas as pd
import numpy as np
import websockets

# Try import RPi.GPIO; if not available, use simulation/manual only.
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except Exception:
    GPIO_AVAILABLE = False

# Defaults
PORT = 8765
TICK_HZ = 5
LOG_DIR = "logs"
GRACE_SECONDS = 5.0
DEFAULT_TOL = 2.0

class PulseCounter:
    """Simple thread-safe deque of pulse timestamps (monotonic)."""
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
    def __init__(self, df, tick_hz=TICK_HZ, debounce=0.5, circ=1.94, ppr=1.0, grace=GRACE_SECONDS, gpio_pin=17, use_gpio=False, verbose=False):
        self.profile = df.copy()
        self.tick_hz = tick_hz
        self.dt = 1.0 / tick_hz
        self.debounce = float(debounce)
        self.circ = float(circ)            # meters
        self.ppr = float(ppr)              # pulses per rotation
        self.grace = float(grace)
        self.gpio_pin = int(gpio_pin)
        self.use_gpio = bool(use_gpio) and GPIO_AVAILABLE
        self.verbose = verbose

        # state
        self.running = False
        self.start_monotonic = None
        self.elapsed = 0.0

        # crossing/violation tracking
        self.violations = 0
        self.prev_inside = True
        self.last_cross_monotonic = None

        # speed source
        self.mode = "manual"   # "manual" or "real"
        self.manual_speed = 0.0
        self.actual_speed = 0.0

        # pulse counting for real sensor
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

        # setup GPIO if asked
        if self.use_gpio:
            self._setup_gpio()

    def _setup_gpio(self):
        try:
            GPIO.setmode(GPIO.BCM)
            GPIO.setup(self.gpio_pin, GPIO.IN, pull_up_down=GPIO.PUD_OFF)
            GPIO.add_event_detect(self.gpio_pin, GPIO.FALLING, callback=self._gpio_cb, bouncetime=10)
            if self.verbose:
                print(f"[GPIO] configured BCM{self.gpio_pin}")
        except Exception as e:
            print("[GPIO] setup failed:", e)
            self.use_gpio = False

    def _gpio_cb(self, ch):
        # called in a separate thread by RPi.GPIO
        self.pulse_counter.add()
        if self.verbose:
            print(f"[GPIO] pulse at {time.monotonic():.3f}")

    def interp_profile(self, t):
        if t <= self.times[0]:
            return float(self.targets[0]), float(self.uppers[0]), float(self.lowers[0])
        if t >= self.times[-1]:
            return float(self.targets[-1]), float(self.uppers[-1]), float(self.lowers[-1])
        tg = float(np.interp(t, self.times, self.targets))
        up = float(np.interp(t, self.times, self.uppers))
        lo = float(np.interp(t, self.times, self.lowers))
        return tg, up, lo

    # websocket helpers
    async def register(self, ws):
        self.clients.add(ws)
        prof_msg = {
            "type": "profile",
            "profile": {
                "time": self.times.tolist(),
                "target": self.targets.tolist(),
                "upper": self.uppers.tolist(),
                "lower": self.lowers.tolist()
            }
        }
        try:
            await ws.send(json.dumps(prof_msg))
            # send initial snapshot
            await ws.send(json.dumps(self.snapshot()))
            if self.verbose:
                print("[WS] profile & snapshot sent to client")
        except Exception:
            pass

    def unregister(self, ws):
        self.clients.discard(ws)

    async def handler(self, websocket, path):
        # Correct signature: (websocket, path)
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
                self.prev_inside = True
                self.last_cross_monotonic = None
                self._open_log()
                if self.verbose: print("[CMD] start")
        elif cmd == "stop":
            if self.running:
                self.running = False
                self._close_log()
                if self.verbose: print("[CMD] stop")
        elif cmd == "reset":
            self.running = False
            self.elapsed = 0.0
            self.violations = 0
            self.prev_inside = True
            self.last_cross_monotonic = None
            self._close_log()
            await self.broadcast({"type":"reset"})
            if self.verbose: print("[CMD] reset")
        elif cmd == "set_mode":
            m = obj.get("mode")
            if m in ("manual","real"):
                self.mode = m
                if self.verbose: print("[CMD] set_mode", m)
        elif cmd == "manual_speed":
            try:
                self.manual_speed = float(obj.get("speed", 0.0))
            except Exception:
                pass

    # logging
    def _open_log(self):
        try:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fname = f"test_{ts}.csv"
            self.logfile = open(os.path.join(LOG_DIR, fname), "w", newline='')
            self.csv_writer = csv.writer(self.logfile)
            self.csv_writer.writerow(["iso","epoch_ms","time_s","target","upper","lower","actual","violations"])
            if self.verbose: print(f"[LOG] opened {fname}")
        except Exception as e:
            print("[LOG] open failed:", e)
            self.logfile = None
            self.csv_writer = None

    def _write_log_row(self, elapsed, target, upper, lower, actual, violations):
        if not self.csv_writer:
            return
        try:
            self.csv_writer.writerow([datetime.utcnow().isoformat(), int(time.time()*1000), round(elapsed,3), round(target,3), round(upper,3), round(lower,3), round(actual,3), int(violations)])
            self.logfile.flush()
        except Exception:
            pass

    def _close_log(self):
        if self.logfile:
            try:
                self.logfile.close()
            except:
                pass
        self.logfile = None
        self.csv_writer = None

    # compute speed from pulses (pulses in last 1s)
    def compute_speed(self):
        window = 1.0
        pulses = self.pulse_counter.count_recent(window)
        pps = pulses / window
        rotations_per_sec = pps / max(1.0, self.ppr)
        speed_mps = rotations_per_sec * self.circ
        return speed_mps * 3.6

    async def broadcast(self, obj):
        if not self.clients:
            return
        data = json.dumps(obj)
        coros = []
        for ws in set(self.clients):
            try:
                coros.append(ws.send(data))
            except Exception:
                self.unregister(ws)
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    def snapshot(self):
        return {"type":"update","time":round(self.elapsed,2),"target":None,"upper":None,"lower":None,"actual":round(self.actual_speed,2),"violations":int(self.violations),"running":bool(self.running)}

    async def run_loop(self):
        if self.verbose:
            print(f"[BACKEND] loop {self.tick_hz}Hz profile_end={self.profile_end}s GPIO={'on' if self.use_gpio else 'off'} debounce={self.debounce}s circ={self.circ} ppr={self.ppr}")
        while True:
            tick_start = time.monotonic()

            if self.running:
                self.elapsed = time.monotonic() - (self.start_monotonic or time.monotonic())
                if self.elapsed < 0: self.elapsed = 0.0

                # choose speed source
                if self.mode == "manual" or not self.use_gpio:
                    self.actual_speed = float(self.manual_speed)
                else:
                    self.actual_speed = self.compute_speed()

                target, upper, lower = self.interp_profile(self.elapsed)

                # immediate crossing detection: inside -> outside triggers violation
                inside = (self.actual_speed >= lower) and (self.actual_speed <= upper)
                crossed_event = (not inside) and self.prev_inside
                now = time.monotonic()
                crossed_flag = False

                if self.elapsed <= self.grace:
                    # ignore crossings in grace
                    if crossed_event and self.verbose:
                        print(f"[VIOL] crossed during grace at {self.elapsed:.2f}s (ignored)")
                    self.prev_inside = inside
                else:
                    if crossed_event:
                        # debounce
                        if (self.last_cross_monotonic is None) or (now - self.last_cross_monotonic >= self.debounce):
                            self.violations += 1
                            self.last_cross_monotonic = now
                            crossed_flag = True
                            if self.verbose:
                                side = "upper" if self.actual_speed > upper else "lower"
                                print(f"[VIOL] CROSS #{self.violations} @ {self.elapsed:.2f}s actual={self.actual_speed:.2f} crossed {side}")
                        else:
                            if self.verbose:
                                print(f"[VIOL] crossed but debounce active dt={now-self.last_cross_monotonic:.3f}s")
                    self.prev_inside = inside

                # broadcast update (include crossed flag)
                msg = {
                    "type": "update",
                    "time": round(self.elapsed,2),
                    "target": round(target,2),
                    "upper": round(upper,2),
                    "lower": round(lower,2),
                    "actual": round(self.actual_speed,2),
                    "violations": int(self.violations),
                    "running": True,
                    "crossed": bool(crossed_flag)
                }
                self._write_log_row(self.elapsed, target, upper, lower, self.actual_speed, self.violations)
                await self.broadcast(msg)

                # stop if profile end reached
                if self.elapsed >= self.profile_end:
                    await self.broadcast({"type":"complete","time":round(self.elapsed,2),"violations":int(self.violations)})
                    self.running = False
                    self._close_log()

            # maintain tick rate
            tick_end = time.monotonic()
            elapsed = tick_end - tick_start
            await asyncio.sleep(max(0.0, self.dt - elapsed))

# Main
async def main(profile_path, host='0.0.0.0', port=PORT, tol=DEFAULT_TOL, rebase=False, debug=False, gpio_pin=17, circ=1.94, ppr=1.0, use_gpio=False, debounce=0.5):
    df = pd.read_csv(profile_path)
    if 'time' not in df.columns or 'target' not in df.columns:
        raise SystemExit("Profile CSV must contain at least 'time' and 'target' columns.")
    if 'upper' not in df.columns:
        df['upper'] = df['target'] + tol
    if 'lower' not in df.columns:
        df['lower'] = df['target'] - tol
    df = df[['time','target','upper','lower']].sort_values('time').reset_index(drop=True)

    if rebase:
        t0 = df['time'].iloc[0]
        df['time'] = df['time'] - t0
        if debug:
            print(f"[MAIN] rebase by {t0}s -> new start {df['time'].iloc[0]}s")

    backend = DriveBackend(df, tick_hz=TICK_HZ, debounce=debounce, circ=circ, ppr=ppr, grace=GRACE_SECONDS, gpio_pin=gpio_pin, use_gpio=use_gpio, verbose=debug)

    try:
        server = await websockets.serve(backend.handler, host, port)
    except OSError as e:
        print("Fatal error: could not bind websocket port:", e)
        print("Maybe another backend is running. Run: sudo pkill -f backend.py  OR choose a different --port.")
        raise

    print(f"[MAIN] WebSocket server running on ws://{host}:{port}  GPIO={'on' if backend.use_gpio else 'off'}")
    loop = asyncio.get_running_loop()
    loop.create_task(backend.run_loop())

    # graceful wait for signals
    stop = asyncio.Future()
    def _on_sig():
        if not stop.done():
            stop.set_result(None)

    try:
        loop.add_signal_handler(signal.SIGINT, _on_sig)
        loop.add_signal_handler(signal.SIGTERM, _on_sig)
    except NotImplementedError:
        # Windows or limited environment: do nothing
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
    p = argparse.ArgumentParser()
    p.add_argument('--profile', required=True, help='CSV profile path (time, target, optionally upper/lower)')
    p.add_argument('--host', default='0.0.0.0')
    p.add_argument('--port', type=int, default=PORT)
    p.add_argument('--tol', type=float, default=DEFAULT_TOL)
    p.add_argument('--rebase', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--gpio-pin', type=int, default=17)
    p.add_argument('--circ', type=float, default=1.94, help='wheel circumference in meters')
    p.add_argument('--ppr', type=float, default=1.0, help='pulses per wheel rotation')
    p.add_argument('--use-gpio', action='store_true')
    p.add_argument('--debounce', type=float, default=0.5, help='seconds to ignore subsequent crossings after a counted crossing')
    args = p.parse_args()

    print("[MAIN] starting backend with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol, rebase=args.rebase, debug=args.debug, gpio_pin=args.gpio_pin, circ=args.circ, ppr=args.ppr, use_gpio=args.use_gpio, debounce=args.debounce))
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print("Fatal:", e)
        raise
