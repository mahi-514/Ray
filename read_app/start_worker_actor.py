import ray

ray.init(address="auto", namespace="file-processing")

from master_worker import MasterActor

# Try to create master and catch error if it fails
try:
    MasterActor.options(
        name="master",
        lifetime="detached",
        namespace="file-processing"
    ).remote()
    print("[INFO] Master actor created.")
except Exception as e:
    print("[ERROR] Failed to create MasterActor:", e)
