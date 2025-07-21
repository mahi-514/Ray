import ray
import pandas as pd
import time
import os
import uuid
import threading
import sys
import django


sys.path.append('/mnt/c/Users/Public/Mahi/ray_django_project/read_file')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "read_file.settings")
django.setup()

from read_app.models import RunLogTable

@ray.remote(resources={"worker_abc1": 1}, max_concurrency=10)
class WorkerActor:
    def __init__(self):
        import sys
        import os
        import django

        sys.path.append('/mnt/c/Users/Public/Mahi/ray_django_project/read_file')
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "read_file.settings")
        django.setup()

        from read_app.models import RunLogTable
        self.RunLogTable = RunLogTable
        self.busy = False

    def is_busy(self):
        return self.busy

    def run_task(self, file_path, ray_id):
        try:
            self.busy = True
            self.RunLogTable.objects.filter(ray_id=ray_id).update(status="Running")
            print(f"[Worker] Running task: {ray_id}")

            import time, os, pandas as pd
            time.sleep(10)  # Simulate heavy task

            if not os.path.exists(file_path):
                raise FileNotFoundError("File not found")

            df = pd.read_csv(file_path)
            result = df.to_dict()

            self.RunLogTable.objects.filter(ray_id=ray_id).update(status="Success")
            return {"status": "Success", "ray_id": ray_id, "data": result}

        except Exception as e:
            self.RunLogTable.objects.filter(ray_id=ray_id).update(status="Failed")
            return {"status": "Failed", "error": str(e), "ray_id": ray_id}
        finally:
            self.busy = False

@ray.remote
class MasterActor:
    def __init__(self):
        import os
        import sys
        import django

        sys.path.append('/mnt/c/Users/Public/Mahi/ray_django_project/read_file')
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "read_file.settings")
        django.setup()

        from read_app.models import RunLogTable
        self.RunLogTable = RunLogTable
        self.worker = None

    def delegate_task(self, file_path, ray_id):
        if self.worker is None:
            try:
                self.worker = ray.get_actor("worker", namespace="file-processing")
            except Exception as e:
                return {"status": "Failed", "error": f"Worker not available: {e}"}

        while ray.get(self.worker.is_busy.remote()):
            print(f"[Master] Worker busy. Waiting for task {ray_id}...")
            time.sleep(1)

        self.RunLogTable.objects.update_or_create(ray_id=ray_id, defaults={"status": "Queued"})
        future = self.worker.run_task.remote(file_path, ray_id)
        return future


if __name__ == "__main__":
    ray.init(address="auto", namespace="file-processing")

    # Create worker actor if not exists
    try:
        ray.get_actor("worker", namespace="file-processing")
        print("[INFO] Worker actor already exists.")
    except ValueError:
        WorkerActor.options(name="worker", lifetime="detached", resources={"worker_abc1": 1}).remote()
        print("[INFO] Worker actor created.")

    # Create master actor if not exists
    try:
        ray.get_actor("master", namespace="file-processing")
        print("[INFO] Master actor already exists.")
    except ValueError:
        MasterActor.options(name="master", lifetime="detached").remote()
        print("[INFO] Master actor created.")
