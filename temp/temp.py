#!/usr/bin/env python3
"""
backend.py

Immediate crossing violation logic + CSV logging + per-second CSV export via WebSocket.

Usage examples:
  python3 backend.py --profile drive_cycle.csv --rebase --debounce 0.0 --debug
  sudo python3 backend.py --profile drive_cycle.csv --use-gpio --gpio-pin 17 --circ 1.94 --ppr 1 --debug

Notes:
 - Writes logs/test_<ts>.csv (every tick)
 - Writes logs/live_seconds.csv (one row per elapsed second; updated as run progresses)
 - WebSocket command: {"cmd":"request_log"} -> backend replies with {"type":"log_csv","csv": "<text base64-safe or raw text>"}
 - Violation logic: count immediate inside->outside crossing (subject to debounce and min_speed guard for lower crossings)
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

import numpy as np
import pandas as pd
import websockets

# Optional RPi GPIO
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except Exception:
    GPIO_AVAILABLE = False

# defaults
PORT = 8765
TICK_HZ = 5
LOG_DIR = "logs"
DEFAULT_TOL = 2.0

class PulseCounter:
    def __init__(self, keep_seconds=10.0):
        self.keep_seconds = keep_seconds
        self.deque = deque()
    def add(self, t=None):
        if t is None:
            t = time.monotonic()
        self.deque.append(t)
        cutoff = t - self.keep_seconds
        while self.deque and self.deque[0] < cutoff:
            self.deque.popleft()
    def count_recent(self, window=1.0):
        now = time.monotonic()
        cutoff = now - window
        cnt = 0
        for ts in reversed(self.deque):
            if ts >= cutoff:
                cnt += 1
            else:
                break
        return cnt

class DriveBackend:
    def __init__(self, df, tick_hz=TICK_HZ, debounce=0.0, circ=1.94, ppr=1.0,
                 gpio_pin=17, use_gpio=False, min_speed=0.0, debug=False):
        self.profile = df.copy()
        self.tick_hz = tick_hz
        self.dt = 1.0 / tick_hz
        self.debounce = float(debounce)
        self.circ = float(circ)
        self.ppr = float(ppr)
        self.gpio_pin = int(gpio_pin)
        self.use_gpio = bool(use_gpio) and GPIO_AVAILABLE
        self.min_speed = float(min_speed)
        self.debug = bool(debug)

        # runtime state
        self.running = False
        self.start_monotonic = None
        self.elapsed = 0.0

        # violation state
        self.violations = 0
        self.prev_inside = True
        self.last_cross_monotonic = None

        # speed source
        self.mode = "manual"
        self.manual_speed = 0.0
        self.actual_speed = 0.0

        # pulses for real sensor
        self.pulse_counter = PulseCounter(keep_seconds=max(10, int(self.tick_hz*5)))

        # websocket clients
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
        self.live_seconds_path = os.path.join(LOG_DIR, "live_seconds.csv")
        self.last_written_second = -1  # to track per-second writing

        if self.use_gpio:
            self._setup_gpio()

    def _setup_gpio(self):
        try:
            GPIO.setmode(GPIO.BCM)
            GPIO.setup(self.gpio_pin, GPIO.IN, pull_up_down=GPIO.PUD_OFF)
            GPIO.add_event_detect(self.gpio_pin, GPIO.FALLING, callback=self._gpio_cb, bouncetime=10)
            if self.debug:
                print(f"[GPIO] configured BCM{self.gpio_pin}")
        except Exception as e:
            print("[GPIO] setup failed:", e)
            self.use_gpio = False

    def _gpio_cb(self, ch):
        self.pulse_counter.add()
        if self.debug:
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

    # websocket registration
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
        # Accept incoming messages and commands
        await self.register(websocket)
        try:
            async for raw in websocket:
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                await self.handle_command(obj, websocket)
        except websockets.ConnectionClosed:
            pass
        finally:
            self.unregister(websocket)

    async def handle_command(self, obj, websocket=None):
        cmd = obj.get("cmd")
        if cmd == "start":
            if not self.running:
                self.running = True
                self.start_monotonic = time.monotonic() - self.elapsed
                # initialize prev_inside based on current actual vs band
                if (self.mode == "manual") or (not self.use_gpio):
                    now_actual = float(self.manual_speed)
                else:
                    now_actual = self.compute_speed()
                self.actual_speed = now_actual
                target, upper, lower = self.interp_profile(self.elapsed)
                self.prev_inside = (self.actual_speed >= lower and self.actual_speed <= upper)
                # if starting outside, count immediately (unless tiny lower)
                if not self.prev_inside:
                    side = "upper" if self.actual_speed > upper else "lower"
                    if side == "lower" and self.actual_speed <= self.min_speed:
                        if self.debug:
                            print("[VIOL] start outside ignored due to min_speed")
                    else:
                        self.violations += 1
                        self.last_cross_monotonic = time.monotonic()
                        # send immediate violation message
                        await self.broadcast({"type":"violation","time":round(self.elapsed,2),"violations":int(self.violations),"side":side,"actual":round(self.actual_speed,2)})
                        if self.debug:
                            print(f"[VIOL] start counted #{self.violations} side={side}")
                if self.debug:
                    print(f"[CMD] start (elapsed {self.elapsed:.2f})")
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
            self.last_written_second = -1
            self._close_log()
            await self.broadcast({"type":"reset"})
            if self.debug:
                print("[CMD] reset")
        elif cmd == "set_mode":
            m = obj.get("mode")
            if m in ("manual","real"):
                self.mode = m
                if self.debug: print("[CMD] set_mode", m)
        elif cmd == "manual_speed":
            try:
                self.manual_speed = float(obj.get("speed", 0.0))
            except Exception:
                pass
        elif cmd == "request_log":
            # send current live_seconds.csv content
            try:
                if os.path.exists(self.live_seconds_path):
                    with open(self.live_seconds_path, "r") as f:
                        csv_text = f.read()
                else:
                    csv_text = ""
                await self.broadcast({"type":"log_csv","csv": csv_text})
            except Exception as e:
                await self.broadcast({"type":"log_csv","csv": "", "error": str(e)})
        # else: ignore unknown

    # logging
    def _open_log(self):
        try:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fname = f"test_{ts}.csv"
            self.logfile = open(os.path.join(LOG_DIR, fname), "w", newline='')
            self.csv_writer = csv.writer(self.logfile)
            self.csv_writer.writerow(["iso","epoch_ms","time_s","target","upper","lower","actual","violations"])
            # create/overwrite live_seconds CSV with header
            with open(self.live_seconds_path, "w", newline='') as f:
                w = csv.writer(f)
                w.writerow(["iso","epoch_ms","time_s","target","upper","lower","actual","violations"])
            if self.debug:
                print(f"[LOG] opened {fname} and live_seconds.csv")
        except Exception as e:
            print("[LOG] open failed:", e)
            self.logfile = None
            self.csv_writer = None

    def _write_log_row(self, elapsed, target, upper, lower, actual, violations):
        row = [datetime.utcnow().isoformat(), int(time.time()*1000), round(elapsed,3), round(target,3), round(upper,3), round(lower,3), round(actual,3), int(violations)]
        # CSV (every tick)
        if self.csv_writer:
            try:
                self.csv_writer.writerow(row)
                self.logfile.flush()
            except Exception:
                pass
        # per-second file: write only once per integer second
        sec = int(elapsed)
        if sec != self.last_written_second:
            try:
                with open(self.live_seconds_path, "a", newline='') as f:
                    w = csv.writer(f)
                    w.writerow(row)
                self.last_written_second = sec
            except Exception as e:
                if self.debug: print("[LOG] live_seconds write failed:", e)

    def _close_log(self):
        if self.logfile:
            try:
                self.logfile.close()
            except Exception:
                pass
        self.logfile = None
        self.csv_writer = None

    def compute_speed(self):
        # convert pulses-per-second to km/h
        window = 1.0
        pulses = self.pulse_counter.count_recent(window)
        pps = pulses / window
        rps = pps / max(1.0, self.ppr)
        speed_mps = rps * self.circ
        return speed_mps * 3.6

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
            print(f"[BACKEND] running @ {self.tick_hz}Hz profile_end={self.profile_end}s GPIO={'on' if self.use_gpio else 'off'} debounce={self.debounce}s min_speed={self.min_speed}")
        while True:
            tick_start = time.monotonic()
            if self.running:
                self.elapsed = time.monotonic() - (self.start_monotonic or time.monotonic())
                if self.elapsed < 0: self.elapsed = 0.0

                # speed source
                if self.mode == "manual" or not self.use_gpio:
                    self.actual_speed = float(self.manual_speed)
                else:
                    self.actual_speed = self.compute_speed()

                target, upper, lower = self.interp_profile(self.elapsed)

                inside = (self.actual_speed >= lower) and (self.actual_speed <= upper)
                crossed_event = (not inside) and self.prev_inside
                nowm = time.monotonic()
                crossed_flag = False
                cross_side = None

                if crossed_event:
                    cross_side = "upper" if self.actual_speed > upper else "lower"
                    # ignore tiny lower crossings
                    if cross_side == "lower" and self.actual_speed <= self.min_speed:
                        if self.debug:
                            print(f"[VIOL] ignored tiny lower at {self.elapsed:.2f}s actual={self.actual_speed:.2f} <= min_speed {self.min_speed}")
                        self.prev_inside = inside
                    else:
                        # apply debounce (do not count multiple rapid crosses)
                        if (self.last_cross_monotonic is None) or (nowm - self.last_cross_monotonic >= self.debounce):
                            self.violations += 1
                            self.last_cross_monotonic = nowm
                            crossed_flag = True
                            # immediate violation broadcast
                            await self.broadcast({"type":"violation","time":round(self.elapsed,2),"violations":int(self.violations),"side":cross_side,"actual":round(self.actual_speed,2)})
                            if self.debug:
                                print(f"[VIOL] CROSS #{self.violations} at {self.elapsed:.2f}s actual={self.actual_speed:.2f} side={cross_side}")
                        else:
                            if self.debug:
                                print(f"[VIOL] crossing ignored by debounce ({nowm-self.last_cross_monotonic:.3f}s)")
                        self.prev_inside = inside
                else:
                    # update prev_inside when inside
                    if inside:
                        self.prev_inside = True

                # log and broadcast update
                self._write_log_row(self.elapsed, target, upper, lower, self.actual_speed, self.violations)

                msg = {
                    "type":"update",
                    "time": round(self.elapsed,2),
                    "target": round(target,2),
                    "upper": round(upper,2),
                    "lower": round(lower,2),
                    "actual": round(self.actual_speed,2),
                    "violations": int(self.violations),
                    "running": True,
                    "crossed": bool(crossed_flag),
                    "cross_side": cross_side if crossed_flag else None
                }
                await self.broadcast(msg)

                # profile complete handling
                if self.elapsed >= self.profile_end:
                    await self.broadcast({"type":"complete","time":round(self.elapsed,2),"violations":int(self.violations)})
                    self.running = False
                    self._close_log()

            # sleep to maintain tick rate
            tick_end = time.monotonic()
            elapsed_tick = tick_end - tick_start
            await asyncio.sleep(max(0.0, self.dt - elapsed_tick))

async def main(profile_path, host='0.0.0.0', port=PORT, tol=DEFAULT_TOL, rebase=False, debug=False,
               gpio_pin=17, circ=1.94, ppr=1.0, use_gpio=False, debounce=0.0, min_speed=0.0):
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
                           gpio_pin=gpio_pin, use_gpio=use_gpio, min_speed=min_speed, debug=debug)
    try:
        server = await websockets.serve(backend.handler, host, port)
    except OSError as e:
        print("Fatal: could not bind websocket port:", e)
        raise

    print(f"[MAIN] WebSocket server ws://{host}:{port}  GPIO={'on' if backend.use_gpio else 'off'}")
    loop = asyncio.get_running_loop()
    loop.create_task(backend.run_loop())

    # signal handling
    stop = asyncio.Future()
    def _on_sig():
        if not stop.done(): stop.set_result(None)
    try:
        loop.add_signal_handler(signal.SIGINT, _on_sig)
        loop.add_signal_handler(signal.SIGTERM, _on_sig)
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
    parser.add_argument('--debounce', type=float, default=0.0)
    parser.add_argument('--min-speed', type=float, default=0.0, help='ignore lower crossings below this actual speed (km/h)')
    args = parser.parse_args()

    print("[MAIN] backend starting with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol, rebase=args.rebase,
                         debug=args.debug, gpio_pin=args.gpio_pin, circ=args.circ, ppr=args.ppr,
                         use_gpio=args.use_gpio, debounce=args.debounce, min_speed=args.min_speed))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Fatal:", e)
        raise
