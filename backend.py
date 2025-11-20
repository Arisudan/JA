import asyncio
import json
import argparse
import time
from datetime import datetime
import pandas as pd
import numpy as np
import websockets
import os
import csv
import signal
import sys
import io

# Config
PORT = 8765
TICK_HZ = 5
GRACE_SECONDS = 5.0
DEFAULT_TOL = 2.0    # company asked +/- 2 km/h by default
LOG_DIR = "logs"

class DriveBackend:
    def __init__(self, profile_df, tick_hz=TICK_HZ, grace=GRACE_SECONDS, tol=DEFAULT_TOL):
        self.profile = profile_df.copy()
        self.tick_hz = tick_hz
        self.dt = 1.0 / tick_hz
        self.grace = grace
        self.default_tol = tol

        # runtime
        self.running = False
        self.start_wall = None
        self.elapsed = 0.0
        self.violations = 0
        self._in_violation = False

        self.mode = "manual"
        self.manual_speed = 0.0
        self.actual_speed = 0.0

        self.clients = set()

        self.times = self.profile['time'].values.astype(float)
        self.targets = self.profile['target'].values.astype(float)
        self.uppers = self.profile['upper'].values.astype(float)
        self.lowers = self.profile['lower'].values.astype(float)
        self.profile_end = float(self.times[-1]) if len(self.times) else 0.0

        os.makedirs(LOG_DIR, exist_ok=True)
        self.logfile = None
        self.csv_writer = None
        self.current_test_id = None
    
    def load_profile_from_csv_data(self, csv_data, tol=None):
        """Load a new profile from CSV data string"""
        if tol is None:
            tol = self.default_tol
        
        try:
            # Parse CSV data
            df = pd.read_csv(io.StringIO(csv_data))
            
            if 'time' not in df.columns or 'target' not in df.columns:
                raise ValueError("CSV must contain at least 'time' and 'target' columns.")
            
            # Add upper/lower bounds if not present
            if 'upper' not in df.columns:
                df['upper'] = df['target'] + tol
            if 'lower' not in df.columns:
                df['lower'] = df['target'] - tol
            
            # Clean and sort data
            df = df[['time', 'target', 'upper', 'lower']].sort_values('time').reset_index(drop=True)
            
            # Stop current test if running
            was_running = self.running
            if self.running:
                self.running = False
                self._close_log()
            
            # Update profile data
            self.profile = df.copy()
            self.times = self.profile['time'].values.astype(float)
            self.targets = self.profile['target'].values.astype(float)
            self.uppers = self.profile['upper'].values.astype(float)
            self.lowers = self.profile['lower'].values.astype(float)
            self.profile_end = float(self.times[-1]) if len(self.times) else 0.0
            
            # Reset state
            self.elapsed = 0.0
            self.violations = 0
            self._in_violation = False
            
            return True, "Profile loaded successfully"
            
        except Exception as e:
            return False, f"Error loading profile: {str(e)}"

    def interp_profile(self, t):
        if t <= self.times[0]:
            return float(self.targets[0]), float(self.uppers[0]), float(self.lowers[0])
        if t >= self.times[-1]:
            return float(self.targets[-1]), float(self.uppers[-1]), float(self.lowers[-1])
        target = float(np.interp(t, self.times, self.targets))
        upper = float(np.interp(t, self.times, self.uppers))
        lower = float(np.interp(t, self.times, self.lowers))
        return target, upper, lower

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
                self.start_wall = time.time() - self.elapsed
                self._in_violation = False
                self._open_log()
        elif cmd == "stop":
            if self.running:
                self.running = False
                self._close_log()
        elif cmd == "reset":
            self.running = False
            self.elapsed = 0.0
            self.violations = 0
            self._in_violation = False
            self.actual_speed = 0.0
            self.manual_speed = 0.0
            self._close_log()
            await self.broadcast({"type": "reset"})
            # Send updated state with all values reset
            reset_update = {
                "type": "update",
                "time": 0.0,
                "target": 0.0,
                "actual": 0.0,
                "violations": 0,
                "running": False
            }
            await self.broadcast(reset_update)
        elif cmd == "set_mode":
            m = obj.get("mode")
            if m in ("manual", "real"):
                self.mode = m
        elif cmd == "manual_speed":
            try:
                self.manual_speed = float(obj.get("speed", 0.0))
            except Exception:
                pass
        elif cmd == "load_profile":
            csv_data = obj.get("csv_data")
            filename = obj.get("filename", "uploaded.csv")
            if csv_data:
                success, message = self.load_profile_from_csv_data(csv_data)
                if success:
                    # Send new profile to all clients
                    prof = {
                        "type": "profile",
                        "profile": {
                            "time": self.times.tolist(),
                            "target": self.targets.tolist(),
                            "upper": self.uppers.tolist(),
                            "lower": self.lowers.tolist()
                        }
                    }
                    await self.broadcast(prof)
                    await self.broadcast({"type": "reset"})
                    await self.broadcast({"type": "profile_loaded", "filename": filename, "message": message})
                    print(f"Profile loaded from {filename}: {len(self.times)} points, duration {self.profile_end:.1f}s")
                else:
                    await self.broadcast({"type": "error", "message": message})
                    print(f"Failed to load profile from {filename}: {message}")

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

    async def run_loop(self):
        print(f"Backend loop starting ({self.tick_hz} Hz). Profile end: {self.profile_end}s")
        while True:
            t0 = time.time()
            if self.running:
                self.elapsed = time.time() - self.start_wall
                if self.elapsed < 0:
                    self.elapsed = 0.0

                if self.mode == "manual":
                    self.actual_speed = float(self.manual_speed)
                else:
                    self.actual_speed = float(self.manual_speed)

                target, upper, lower = self.interp_profile(self.elapsed)

                out_of_range = (self.actual_speed < lower) or (self.actual_speed > upper)
                if out_of_range and (self.elapsed > self.grace):
                    if not self._in_violation:
                        self._in_violation = True
                        # start violation window
                    else:
                        self.violations += 1
                        self._in_violation = False
                else:
                    self._in_violation = False

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

            t1 = time.time()
            elapsed = t1 - t0
            to_sleep = max(0.0, self.dt - elapsed)
            await asyncio.sleep(to_sleep)

async def main(profile_path, host="0.0.0.0", port=PORT, tol=DEFAULT_TOL):
    df = pd.read_csv(profile_path)
    if 'time' not in df.columns or 'target' not in df.columns:
        raise SystemExit("Profile CSV must contain at least 'time' and 'target' columns.")
    if 'upper' not in df.columns:
        df['upper'] = df['target'] + tol
    if 'lower' not in df.columns:
        df['lower'] = df['target'] - tol
    df = df[['time', 'target', 'upper', 'lower']].sort_values('time').reset_index(drop=True)

    backend = DriveBackend(df, tol=tol)

    # wrapper to avoid websockets signature mismatch
    server = await websockets.serve(lambda ws, path=None: backend.handler(ws, path), host, port)
    print(f"Drive backend websocket server running on ws://{host}:{port}")

    loop = asyncio.get_running_loop()
    loop.create_task(backend.run_loop())

    stop = asyncio.Future()
    def _on_signal(sig_name):
        if not stop.done():
            stop.set_result(None)
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: _on_signal("SIGINT"))
    except NotImplementedError:
        # some platforms may not support add_signal_handler
        pass

    try:
        await stop
    finally:
        server.close()
        await server.wait_closed()
        backend._close_log()
        print("Backend shutdown complete.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--profile', required=True, help='CSV file with columns time,target[,upper,lower]')
    p.add_argument('--host', default='0.0.0.0')
    p.add_argument('--port', type=int, default=PORT)
    p.add_argument('--tol', type=float, default=DEFAULT_TOL)
    args = p.parse_args()

    print("Starting backend with profile:", args.profile)
    try:
        asyncio.run(main(args.profile, host=args.host, port=args.port, tol=args.tol))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Fatal error:", e)
        raise