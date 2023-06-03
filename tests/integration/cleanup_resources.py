# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


import argparse
import json
import os
import subprocess


def cleanup_chaos_mesh(namespace) -> None:
    print(f"Cleaning up chaos mesh in {namespace}")
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")

    output = subprocess.check_output(
        f"tests/integration/ha_tests/scripts/destroy_chaos_mesh.sh {namespace}",
        shell=True,
        env=env,
    )
    for line in output.decode("utf-8").splitlines():
        print(line)


def cleanup_juju_models() -> None:
    for model in _filter_tests_models("admin/test-"):
        print(f"Destroying model {model}")
        delete_cmd = ["juju", "destroy-model", model, "--destroy-storage", "-y"]
        subprocess.check_output(delete_cmd).decode("utf-8")


def cleanup_all(namespace: str) -> None:
    cleanup_chaos_mesh(namespace)
    cleanup_juju_models()


def _filter_tests_models(prefix: str):
    cmd = ["juju", "models", "--format", "json"]
    models_str = subprocess.check_output(cmd).decode("utf-8")

    models = json.loads(models_str)["models"]

    filtered_models = [model["name"] for model in models if model["name"].startswith(prefix)]
    return filtered_models


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup script")
    parser.add_argument("--namespace", help="The k8s namespace to cleanup")
    parser.add_argument(
        "--cleanup",
        choices=["cleanup_after_ha", "juju_models", "all"],
        help="The type of cleanup to perform",
    )

    args = parser.parse_args()

    if args.cleanup == "cleanup_chaos_mesh":
        cleanup_chaos_mesh(args.namespace)
    elif args.cleanup == "juju_models":
        cleanup_juju_models()
    elif args.cleanup == "all":
        cleanup_all(args.namespace)
