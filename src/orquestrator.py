import os
import json
import time
from datetime import datetime


class PipelineOrchestrator:
    def __init__(self, base_path="."):
        self.base_path = base_path
        self.checkpoint_dir = os.path.join(base_path, "data", "checkpoints")
        self.log_path = os.path.join(base_path, "logs", "execution.log")
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def _log(self, level, message, context=None):
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "context": context or {}
        }
        with open(self.log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def _save_checkpoint(self, step):
        path = os.path.join(self.checkpoint_dir, "checkpoint.json")
        with open(path, "w") as f:
            json.dump({"last_step": step}, f)

    def _load_checkpoint(self):
        path = os.path.join(self.checkpoint_dir, "checkpoint.json")
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return json.load(f).get("last_step")

    def _execute_with_retry(self, func, step_name):
        delays = [0.5, 1]
        for attempt in range(len(delays) + 1):
            try:
                result = func()
                self._log("INFO", f"{step_name} completed")
                return result
            except (FileNotFoundError, KeyError, ValueError) as e:
                self._log("ERROR", f"{step_name} failed", {"error": str(e), "attempt": attempt})
                if attempt >= len(delays):
                    raise
                time.sleep(delays[attempt])

    def run(self, steps):
        last_step = self._load_checkpoint()
        start_index = 0

        if last_step:
            for i, (name, _) in enumerate(steps):
                if name == last_step:
                    start_index = i + 1
                    break

        for name, func in steps[start_index:]:
            try:
                self._execute_with_retry(func, name)
                self._save_checkpoint(name)
            except (FileNotFoundError, KeyError, ValueError) as e:
                self._log("CRITICAL", f"Pipeline stopped at {name}", {"error": str(e)})
                raise