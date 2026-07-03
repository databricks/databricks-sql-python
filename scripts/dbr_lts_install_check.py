"""Install the CI-built connector wheel inside a real DBR LTS cluster and smoke-test it.

This is the GitHub-side driver for the "DBR LTS Install" check. It reproduces the
exact customer install path that broke in ES-1960554 (thrift 0.23.0's sdist failing
to build under the old setuptools shipped by DBR LTS runtimes) -- something the
poetry-install-based unit CI cannot see, because it never does a fresh `pip install`
of the built artifact on an LTS toolchain.

Flow (per DBR LTS version x install target):
  1. Upload the wheel to a UC Volume on the target workspace.
  2. Import the smoke notebook into the workspace.
  3. Submit a one-off Job on an ephemeral single-node SINGLE_USER job cluster pinned
     to the given `spark_version`; the notebook pip-installs the wheel (+ extras) and
     runs a SELECT 1 smoke query.
  4. Poll to completion, surface the run's error, and exit non-zero on any non-SUCCESS.

Auth: env DATABRICKS_HOST + DATABRICKS_TOKEN (the azure-prod CI secrets).

Several non-obvious cluster constraints are baked in below and MUST NOT be removed
without re-validating -- see the inline comments and the block at submit():
  - notebook_task (not spark_python_task from a Volume),
  - the cluster must be SINGLE_USER access mode to read /Volumes,
  - the wheel is copied off /Volumes with dbutils.fs.cp inside the notebook,
  - the notebook calls dbutils.library.restartPython() after install.

Example:
  python scripts/dbr_lts_install_check.py \
      --wheel dist/databricks_sql_connector-4.3.0-py3-none-any.whl \
      --smoke-notebook scripts/dbr_lts_smoke_notebook.py \
      --workspace-dir /Users/ci-sp/dbr_lts_install \
      --spark-version 15.4.x-scala2.12 \
      --extras pyarrow \
      --volume /Volumes/peco/default/ci_wheels \
      --run-id "${GITHUB_RUN_ID}-15.4-pyarrow" \
      --http-path "${DATABRICKS_HTTP_PATH}"
"""
import argparse
import base64
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs, workspace

# Terminal life-cycle states -- once the run reaches one of these, polling stops.
_TERMINAL = (
    jobs.RunLifeCycleState.TERMINATED,
    jobs.RunLifeCycleState.SKIPPED,
    jobs.RunLifeCycleState.INTERNAL_ERROR,
)


def upload_wheel(w: WorkspaceClient, local_path: str, remote_path: str) -> None:
    print(f"uploading {local_path} -> {remote_path}", flush=True)
    with open(local_path, "rb") as f:
        w.files.upload(remote_path, f, overwrite=True)


def import_notebook(w: WorkspaceClient, local_path: str, remote_path: str) -> None:
    """Import a source .py notebook into the workspace.

    The cluster identity cannot reliably read a spark_python_task.python_file off a
    UC Volume, but a workspace notebook_task is read via the control plane and works
    -- so the smoke SOURCE lives in the workspace (the wheel still comes from the
    Volume via dbutils.fs.cp inside the notebook).
    """
    print(f"importing notebook {local_path} -> {remote_path}", flush=True)
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()
    w.workspace.mkdirs(remote_path.rsplit("/", 1)[0])
    w.workspace.import_(
        path=remote_path,
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        content=content,
        overwrite=True,
    )


def cleanup(w: WorkspaceClient, wheel_remote: str, nb_remote: str) -> None:
    """Best-effort removal of the per-run artifacts this driver created.

    Each run writes a unique wheel dir on the Volume and a unique notebook in
    the workspace; delete both so they don't accumulate. Never fails the run --
    cleanup errors are logged, not raised. The ephemeral job cluster
    auto-terminates on its own.
    """
    wheel_dir = wheel_remote.rsplit("/", 1)[0]
    # delete_directory requires an empty dir, so remove the wheel file first.
    for what, fn in (
        ("wheel file", lambda: w.files.delete(wheel_remote)),
        ("wheel dir", lambda: w.files.delete_directory(wheel_dir)),
        ("notebook", lambda: w.workspace.delete(nb_remote)),
    ):
        try:
            fn()
            print(f"cleaned up {what}", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"(cleanup of {what} failed, ignoring: {e})", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--wheel", required=True, help="path to the built connector wheel")
    ap.add_argument(
        "--smoke-notebook", required=True, help="path to the smoke notebook .py"
    )
    ap.add_argument(
        "--workspace-dir",
        default=None,
        help="workspace dir to import the notebook into "
        "(default: /Users/<run-as identity>/dbr_lts_install)",
    )
    ap.add_argument(
        "--spark-version", required=True, help="DBR runtime key, e.g. 15.4.x-scala2.12"
    )
    ap.add_argument(
        "--extras",
        default="",
        help="extra to install: '' (base), 'pyarrow', or 'kernel'",
    )
    ap.add_argument(
        "--node-type", default="Standard_DS3_v2", help="cluster node type id"
    )
    ap.add_argument(
        "--volume", required=True, help="UC Volume dir, /Volumes/<cat>/<schema>/<vol>"
    )
    ap.add_argument(
        "--run-id",
        required=True,
        help="unique per-run id for the volume/notebook paths",
    )
    ap.add_argument(
        "--http-path", required=True, help="SQL warehouse http_path for the smoke query"
    )
    ap.add_argument(
        "--timeout", type=int, default=1800, help="seconds to wait for the run"
    )
    args = ap.parse_args()

    host = os.environ["DATABRICKS_HOST"]
    if not host.startswith("http"):
        host = "https://" + host

    # Auth is OAuth M2M (service principal) throughout: the driver -> workspace
    # API (jobs/scim/files), AND the notebook's connector -> SQL warehouse smoke
    # query (via credentials_provider, same SP). A plain PAT is warehouse-scoped
    # and rejected by the workspace REST API; and a PAT was also rejected by the
    # warehouse from inside the cluster -- one M2M identity avoids both.
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not (client_id and client_secret):
        sys.exit(
            "DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are required "
            "(OAuth M2M service principal)."
        )
    print("driver auth: oauth-m2m (service principal)", flush=True)
    w = WorkspaceClient(
        host=host,
        client_id=client_id,
        client_secret=client_secret,
        auth_type="oauth-m2m",
    )

    # The ephemeral cluster runs SINGLE_USER access mode as this identity so it is
    # UC-enabled and can read the wheel off /Volumes. A no-isolation single-node
    # cluster gets "No Unity API token found in Unity Scope" touching /Volumes.
    # The identity also anchors the default notebook-import dir, so nothing about
    # the run-as principal is hardcoded (works for whatever SP owns the token).
    me = w.current_user.me().user_name
    print(f"run-as identity: {me}", flush=True)
    workspace_dir = args.workspace_dir or f"/Users/{me}/dbr_lts_install"

    wheel_remote = f"{args.volume}/{args.run_id}/{os.path.basename(args.wheel)}"
    nb_remote = f"{workspace_dir}/lts_smoke_{args.run_id}"
    try:
        _run(w, args, host, client_id, client_secret, me, wheel_remote, nb_remote)
    finally:
        # Remove the per-run artifacts on every exit path (success, failure,
        # timeout, or sys.exit -- SystemExit propagates through finally).
        cleanup(w, wheel_remote, nb_remote)


def _run(w, args, host, client_id, client_secret, me, wheel_remote, nb_remote):
    upload_wheel(w, args.wheel, wheel_remote)
    import_notebook(w, args.smoke_notebook, nb_remote)

    label = f"{args.spark_version} extras={args.extras or 'base'}"
    print(f"\nsubmitting DBR LTS install job: {label}", flush=True)
    run = w.jobs.submit(
        run_name=f"ci-dbr-lts-install-{args.run_id}",
        tasks=[
            jobs.SubmitTask(
                task_key="lts_install_smoke",
                new_cluster=compute.ClusterSpec(
                    spark_version=args.spark_version,
                    node_type_id=args.node_type,
                    num_workers=0,  # single node
                    # SINGLE_USER access mode -> UC-enabled -> can read /Volumes.
                    data_security_mode=compute.DataSecurityMode.SINGLE_USER,
                    single_user_name=me,
                    spark_conf={
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]",
                    },
                    custom_tags={"ResourceClass": "SingleNode"},
                ),
                notebook_task=jobs.NotebookTask(
                    notebook_path=nb_remote,
                    base_parameters={
                        "wheel": wheel_remote,
                        "extras": args.extras,
                        "server_hostname": host.replace("https://", ""),
                        "http_path": args.http_path,
                        "client_id": client_id,
                        "client_secret": client_secret,
                    },
                ),
            )
        ],
    )
    run_id = run.run_id
    print(f"submitted run_id={run_id}  {host}/#job/run/{run_id}", flush=True)

    deadline = time.time() + args.timeout
    state = None
    consecutive_errors = 0
    while time.time() < deadline:
        # Tolerate transient polling errors (token refresh, network blips): a
        # single failed get_run must NOT sink the whole check with a spurious
        # failure. Only give up after several consecutive errors.
        try:
            state = w.jobs.get_run(run_id).state
            consecutive_errors = 0
        except Exception as e:  # noqa: BLE001
            consecutive_errors += 1
            print(
                f"  [{int(time.time())}] poll error ({consecutive_errors}/5): {e}",
                flush=True,
            )
            if consecutive_errors >= 5:
                print("giving up after 5 consecutive poll errors", flush=True)
                sys.exit(2)
            time.sleep(20)
            continue
        print(
            f"  [{int(time.time())}] life_cycle={state.life_cycle_state} "
            f"result={state.result_state}",
            flush=True,
        )
        if state.life_cycle_state in _TERMINAL:
            break
        time.sleep(20)
    else:
        print(f"TIMEOUT after {args.timeout}s waiting for run {run_id}", flush=True)
        sys.exit(2)

    # Surface the task run's error (cell stdout is not captured here; the notebook
    # folds pip's stderr tail into the raised exception so it lands in error).
    try:
        task_run_id = w.jobs.get_run(run_id).tasks[0].run_id
        out = w.jobs.get_run_output(task_run_id)
        if out.error:
            print("\n===== run error =====\n" + out.error, flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"(could not fetch run output: {e})", flush=True)

    result = state.result_state if state else None
    print(
        f"\nFINAL result_state={result} on {args.spark_version} (extras={args.extras or 'base'})",
        flush=True,
    )
    if result != jobs.RunResultState.SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
