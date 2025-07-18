from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import ray
import pandas as pd
import os
import time
from ray.runtime_context import get_runtime_context
from .models import RunLogTable
import traceback
import uuid
import threading

ray_object_store = {}

if not ray.is_initialized():
    ray.shutdown()
    ray.init(address='auto')


@ray.remote
def readCSVFile(file_path, ray_id):
    try:
        print(f"Task Started: {ray_id}")

        time.sleep(60)  

        if not os.path.exists(file_path):
            raise FileNotFoundError("File not found")

        df = pd.read_csv(file_path)
        return {"status": "Success", "ray_id": ray_id, "data": df.to_dict()}

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {
            "status": "Failed",
            "error": str(e),
            "ray_id": ray_id
        }

class ReadOperations(APIView):
    def post(self, request):
        file_path = request.data.get("file_path")
        if not file_path:
            return Response({"error": "File path not provided"}, status=400)

        ray_id = uuid.uuid4().hex

        future = readCSVFile.remote(file_path, ray_id)
        ray_object_store[ray_id] = future  

        RunLogTable.objects.update_or_create(
            ray_id=ray_id,
            defaults={"status": "Running"}
        )

        try:
            result = ray.get(future)
            RunLogTable.objects.filter(ray_id=ray_id).update(status=result.get("status", "Failed"))
            return Response(result)
        except Exception as e:
            RunLogTable.objects.filter(ray_id=ray_id).update(status="Failed")
            return Response({"error": str(e), "ray_id": ray_id}, status=500)

class AbortExecution(APIView):
    def post(self, request):
        ray_id = request.data.get("ray_id")
        if not ray_id:
            return Response({"error": "ray_id is required"}, status=400)

        try:
            print(ray_object_store)
            object_ref = ray_object_store.get(ray_id)
            if object_ref is None:
                return Response({"error": f"Task with ray_id {ray_id} not found or expired."}, status=404)

            ray.cancel(object_ref, force=True)
            RunLogTable.objects.filter(ray_id=ray_id).update(status="Aborted")
            return Response({"message": f"Task {ray_id} aborted successfully."})
        except Exception as e:
            return Response({
                "error": str(e),
                "traceback": traceback.format_exc()
            }, status=500)
