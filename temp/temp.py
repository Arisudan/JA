#!/usr/bin/env python3
"""
backend.py - Drive-cycle backend (with real-time Excel export)

Features:
 - Existing CSV logging (same as before).
 - Real-time Excel (XLSX) write to logs/live_log.xlsx (buffered to reduce IO).
 - Sends 'update' messages as before; includes cmvr_timer/compliance fields.
 - Options:
     --min-speed (ignore tiny lower crossings)
     --excel-flush (rows buffer before writing Excel)
 - Dependencies: pandas, numpy, websockets, openpyxl (for to_excel)
"""

import argparse, asyncio, csv, json, os, signal, sys, time
from collections import deque
from datetime import datetime
import threading

import numpy as np
import pandas as pd
import websockets

# Optional GPIO
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except Exception:
    GPIO_AVAILABLE = False

# Defaults
PORT = 8765
TICK_HZ = 5
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
    def __init__(self, df, tick_hz= TICK_HZ, debounce=0.0, circ=1.94, ppr=1.0,
                 gpio_pin=17, use_gpio=False, min_speed=0.0, debug=False, excel_flush_rows=10):
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

        # excel flush rows (buffer)
        self.EXCEL_FLUSH_ROWS = int(excel_flush_rows)
        self.excel_buffer = []  # list of dicts to write to excel

        # runtime
        self.running = False
        self.start_monotonic = None
        self.elapsed = 0.0

        # violations + CMVR timer
        self.violations = 0
        self.prev_inside = True
        self.last_cross_monotonic = None
        self.violation_timer = 0.0
        self.last_violation_check = None
        self.cmvr_threshold = 0.20  # 200 ms sustained to count

        # speed source
        self.mode = "manual"
        self.manual_speed = 0.0
        self.actual_speed = 0.0

        # pulses
        self.pulse_counter = PulseCounter(keep_seconds=max(10, int(self.tick_hz*5)))

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
        self.xlsx_path = os.path.join(LOG_DIR, "live_log.xlsx")

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
                # init prev_inside
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
                    print(f"[CMD] start (elapsed {self.elapsed:.2f}) initial_actual={self.actual_speed:.2f} prev_inside={self.prev_inside}")
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
                if self.debug: print("[CMD] set_mode", m)
        elif cmd == "manual_speed":
            try:
                self.manual_speed = float(obj.get("speed", 0.0))
            except Exception:
                pass

    def _open_log(self):
        try:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fname = f"test_{ts}.csv"
            self.logfile = open(os.path.join(LOG_DIR, fname), "w", newline='')
            self.csv_writer = csv.writer(self.logfile)
            self.csv_writer.writerow(["iso","epoch_ms","time_s","target","upper","lower","actual","violations"])
            if self.debug: print(f"[LOG] opened {fname}")
            # reset excel buffer (start fresh for each test)
            self.excel_buffer = []
            # if existing live xlsx exists, leave it -- we overwrite on flush
            if os.path.exists(self.xlsx_path):
                try:
                    os.remove(self.xlsx_path)
                except Exception:
                    pass
        except Exception as e:
            print("[LOG] open failed:", e)
            self.logfile = None
            self.csv_writer = None

    def _write_log_row(self, elapsed, target, upper, lower, actual, violations):
        # CSV row
        if self.csv_writer:
            try:
                self.csv_writer.writerow([datetime.utcnow().isoformat(), int(time.time()*1000), round(elapsed,3), round(target,3), round(upper,3), round(lower,3), round(actual,3), int(violations)])
                self.logfile.flush()
            except Exception:
                pass

        # Buffer row for Excel
        row = {
            "iso": datetime.utcnow().isoformat(),
            "epoch_ms": int(time.time()*1000),
            "time_s": round(elapsed,3),
            "target": round(target,3),
            "upper": round(upper,3),
            "lower": round(lower,3),
            "actual": round(actual,3),
            "violations": int(violations)
        }
        self.excel_buffer.append(row)
        # flush to xlsx every N rows
        if len(self.excel_buffer) >= self.EXCEL_FLUSH_ROWS:
            try:
                df = pd.DataFrame(self.excel_buffer)
                # if file exists, append by reading existing and concatenating (safe)
                if os.path.exists(self.xlsx_path):
                    existing = pd.read_excel(self.xlsx_path)
                    df = pd.concat([existing, df], ignore_index=True)
                df.to_excel(self.xlsx_path, index=False, engine='openpyxl')
                if self.debug:
                    print(f"[XLSX] flushed {len(self.excel_buffer)} rows -> {self.xlsx_path}")
                self.excel_buffer = []
            except Exception as e:
                print("[XLSX] write failed:", e)

    def _close_log(self):
        # final flush CSV
        if self.logfile:
            try: self.logfile.close()
            except: pass
        self.logfile = None
        self.csv_writer = None
        # flush any remaining excel buffer
        if self.excel_buffer:
            try:
                df = pd.DataFrame(self.excel_buffer)
                if os.path.exists(self.xlsx_path):
                    existing = pd.read_excel(self.xlsx_path)
                    df = pd.concat([existing, df], ignore_index=True)
                df.to_excel(self.xlsx_path, index=False, engine='openpyxl')
                if self.debug:
                    print(f"[XLSX] final flush {len(self.excel_buffer)} rows -> {self.xlsx_path}")
                self.excel_buffer = []
            except Exception as e:
                print("[XLSX] final write failed:", e)

    def compute_speed(self):
        window = 1.0
        pulses = self.pulse_counter.count_recent(window)
        pps = pulses / window
        rps = pps / max(1.0, self.ppr)
        speed_mps = rps * self.circ
        return speed_mps * 3.6

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
            print(f"[BACKEND] running @ {self.tick_hz}Hz profile_end={self.profile_end}s GPIO={'on' if self.use_gpio else 'off'} debounce={self.debounce}s")
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

                # CMVR/AIS timer behavior
                nowm = time.monotonic()
                if self.last_violation_check is None:
                    delta_time = 0.0
                    self.last_violation_check = nowm
                else:
                    delta_time = nowm - self.last_violation_check
                    self.last_violation_check = nowm

                inside = (self.actual_speed >= lower) and (self.actual_speed <= upper)
                if not inside:
                    self.violation_timer += delta_time
                    # determine side
                    side = "upper" if self.actual_speed > upper else "lower"
                    # when timer exceeded, count violation (except ignore tiny lower if configured)
                    if self.violation_timer >= self.cmvr_threshold:
                        if not (side == "lower" and self.actual_speed <= self.min_speed):
                            self.violations += 1
                            if self.debug:
                                print(f"[CMVR] VIOLATION #{self.violations} at {self.elapsed:.2f}s side={side} actual={self.actual_speed:.2f} band=({lower:.2f},{upper:.2f})")
                        else:
                            if self.debug:
                                print(f"[CMVR] Ignored tiny lower violation at {self.elapsed:.2f}s actual={self.actual_speed:.2f} <= min_speed {self.min_speed}")
                        self.violation_timer = 0.0
                else:
                    # reset timer while compliant
                    if self.violation_timer > 0 and self.debug:
                        print(f"[CMVR] compliance restored; timer reset (was {self.violation_timer:.3f}s)")
                    self.violation_timer = 0.0

                # write log row (CSV + buffer for excel)
                self._write_log_row(self.elapsed, target, upper, lower, self.actual_speed, self.violations)

                # build and broadcast update (includes cmvr_timer)
                msg = {
                    "type":"update",
                    "time": round(self.elapsed,2),
                    "target": round(target,2),
                    "upper": round(upper,2),
                    "lower": round(lower,2),
                    "actual": round(self.actual_speed,2),
                    "violations": int(self.violations),
                    "running": True,
                    "crossed": False,
                    "cross_side": None,
                    "cmvr_timer": round(self.violation_timer,3),
                    "cmvr_compliant": inside
                }
                await self.broadcast(msg)

                # stop if profile ended
                if self.elapsed >= self.profile_end:
                    await self.broadcast({"type":"complete","time":round(self.elapsed,2),"violations":int(self.violations)})
                    self.running = False
                    self._close_log()

            # tick sleep
            tick_end = time.monotonic()
            elapsed = tick_end - tick_start
            await asyncio.sleep(max(0.0, self.dt - elapsed))

# Main entry
async def main(profile_path, host='0.0.0.0', port=PORT, tol=DEFAULT_TOL, rebase=False, debug=False,
               gpio_pin=17, circ=1.94, ppr=1.0, use_gpio=False, debounce=0.0, min_speed=0.0, excel_flush_rows=10):
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
                           gpio_pin=gpio_pin, use_gpio=use_gpio, min_speed=min_speed, debug=debug, excel_flush_rows=excel_flush_rows)

    try:
        server = await websockets.serve(backend.handler, host, port)
    except OSError as e:
        print("Fatal: could not bind websocket port:", e)
        raise

    print(f"[MAIN] WebSocket server ws://{host}:{port}  GPIO={'on' if backend.use_gpio else 'off'}")
    loop = asyncio.get_running_loop()
    loop.create_task(backend.run_loop())

    stop = asyncio.Future()
    def _on_sig():
        if not stop.done():
            stop.set_result(None)
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
    p = argparse.ArgumentParser()
    p.add_argument('--profile', required=True)
    p.add_argument('--host', default='0.0.0.0')
    p.add_argument('--port', type=int, default=PORT)
    p.add_argument('--tol', type=float, default=DEFAULT_TOL)
    p.add_argument('--rebase', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--gpio-pin', type=int, default=17)
    p.add_argument('--circ', type=float, default=1.94)
    p.add_argument('--ppr', type=float, default=1.0)
    p.add_argument('--use-gpio', action='store_true')
    p.add_argument('--debounce', type=float, default=0.0)
    p.add_argument('--min-speed', type=float, default=0.0, help='ignore tiny lower crossings below this actual speed (km/h)')
    p.add_argument('--excel-flush-rows', type=int, default=10, help='how many rows to buffer before writing to live_log.xlsx')
    args = p.parse_args()

    print("[MAIN] backend starting with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol, rebase=args.rebase, debug=args.debug,
                         gpio_pin=args.gpio_pin, circ=args.circ, ppr=args.ppr, use_gpio=args.use_gpio, debounce=args.debounce, min_speed=args.min_speed, excel_flush_rows=args.excel_flush_rows))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Fatal:", e)
        raise
