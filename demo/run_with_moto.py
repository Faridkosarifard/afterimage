"""Run demo.py without Docker: a moto S3 server with versioning plus a local sqlite catalog."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3
from moto.server import ThreadedMotoServer


def main() -> int:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    try:
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="admin",
            aws_secret_access_key="password",
            region_name="us-east-1",
        )
        client.create_bucket(Bucket="warehouse")
        client.put_bucket_versioning(Bucket="warehouse", VersioningConfiguration={"Status": "Enabled"})
        with tempfile.TemporaryDirectory(prefix="afterimage-demo-") as tmp:
            env = {
                **os.environ,
                "AFTERIMAGE_DEMO_TYPE": "sql",
                "AFTERIMAGE_DEMO_URI": f"sqlite:///{tmp}/catalog.db",
                "AFTERIMAGE_DEMO_WAREHOUSE": "s3://warehouse/demo",
                "AFTERIMAGE_DEMO_S3_ENDPOINT": endpoint,
            }
            demo = Path(__file__).with_name("demo.py")
            return subprocess.run([sys.executable, str(demo)], env=env, check=False).returncode
    finally:
        server.stop()


if __name__ == "__main__":
    sys.exit(main())
