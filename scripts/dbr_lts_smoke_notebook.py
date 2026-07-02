# Databricks notebook source
# DBR LTS install smoke check. Runs INSIDE a job cluster on a DBR LTS runtime,
# driven by scripts/dbr_lts_install_check.py. It reproduces the real customer
# install path that broke in ES-1960554: a fresh `pip install` of the built
# connector wheel, which resolves + builds transitive deps (notably thrift's
# sdist) under the cluster's OWN, old, setuptools -- then imports the connector
# and runs SELECT 1 against the PECO SQL warehouse.
#
# Params arrive as job notebook_task base_parameters -> dbutils widgets.

# COMMAND ----------
import subprocess
import sys

dbutils.widgets.text("wheel", "")
dbutils.widgets.text("extras", "")
dbutils.widgets.text("server_hostname", "")
dbutils.widgets.text("http_path", "")
dbutils.widgets.text("client_id", "")
dbutils.widgets.text("client_secret", "")

wheel = dbutils.widgets.get("wheel")
extras = dbutils.widgets.get("extras")

print("=== runtime toolchain ===")
print("python:", sys.version)
for mod in ("setuptools", "pip", "wheel"):
    try:
        m = __import__(mod)
        print(f"{mod}:", getattr(m, "__version__", "?"))
    except Exception as e:  # noqa: BLE001
        print(f"{mod}: <not importable> {e}")

# COMMAND ----------
# Copy the wheel off the UC Volume to local disk with dbutils.fs (which HAS
# Volume access). Raw open()/shutil on the /Volumes FUSE mount raises
# "PermissionError: [Errno 1] Operation not permitted" from the driver Python,
# and the cluster must be SINGLE_USER access mode or dbutils.fs itself fails
# with "No Unity API token found in Unity Scope". We then pip-install the LOCAL
# copy, so the install itself -- the resolve + sdist build of transitive deps
# under the cluster's OWN toolchain -- is exactly the ES-1960554 path.
local_wheel = "/tmp/" + wheel.rsplit("/", 1)[-1]
dbutils.fs.cp("dbfs:" + wheel, "file:" + local_wheel)
print("copied wheel ->", local_wheel)

target = local_wheel + (f"[{extras}]" if extras else "")
print(f"=== pip install {target} ===")
res = subprocess.run(
    [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", target],
    capture_output=True,
    text=True,
)
print(res.stdout)
print(res.stderr)
if res.returncode != 0:
    # Fold pip's tail into the exception so the reason lands in the job run's
    # error (cell stdout is NOT captured by jobs.get_run_output).
    tail = (res.stdout + "\n" + res.stderr).strip().splitlines()[-25:]
    raise RuntimeError(
        f"pip install failed (rc={res.returncode}) for {target}\n" + "\n".join(tail)
    )

# COMMAND ----------
# Upgrade databricks-sdk for the SMOKE HARNESS only (separate from the wheel
# under test). Older DBR LTS (13.3 / 14.3) ship an SDK too old to honor
# auth_type="oauth-m2m" -- oauth_service_principal falls through to
# DefaultCredentials and raises "cannot configure default credentials". A
# current SDK fixes the M2M credential provider used by the smoke query below.
sdk_res = subprocess.run(
    [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "-U",
        "databricks-sdk",
    ],
    capture_output=True,
    text=True,
)
print(sdk_res.stdout[-2000:])
if sdk_res.returncode != 0:
    tail = (sdk_res.stdout + "\n" + sdk_res.stderr).strip().splitlines()[-15:]
    raise RuntimeError("databricks-sdk upgrade failed\n" + "\n".join(tail))

# COMMAND ----------
# DBR pre-seeds a `databricks` namespace package at
# /databricks/spark/python/databricks ahead of the pip-installed connector on
# sys.path, so `from databricks import sql` raises ImportError until Python is
# restarted. This is the supported way to make a notebook-scoped install take
# effect. NOTE: all Python state is wiped -- widgets are re-read below.
dbutils.library.restartPython()

# COMMAND ----------
# Fresh interpreter: re-read widgets, then import + smoke query.
server_hostname = dbutils.widgets.get("server_hostname")
http_path = dbutils.widgets.get("http_path")
client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")

from importlib.metadata import version

print("databricks-sql-connector:", version("databricks-sql-connector"))
print("thrift:", version("thrift"))

print("=== smoke query (SELECT 1) ===")
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal


# OAuth M2M (service principal) -- same identity the driver uses. Avoids
# depending on a warehouse-scoped PAT (which the SQL warehouse rejected as
# "Invalid access token" from inside the cluster). Mirrors examples/m2m_oauth.py.
def credential_provider():
    return oauth_service_principal(
        Config(
            host=f"https://{server_hostname}",
            client_id=client_id,
            client_secret=client_secret,
            # Explicit so an ambient DATABRICKS_TOKEN on the cluster doesn't
            # collide ("more than one authorization method configured").
            auth_type="oauth-m2m",
        )
    )


with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    credentials_provider=credential_provider,
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        row = cur.fetchone()
        assert row is not None and row[0] == 1, f"unexpected result: {row!r}"
        print("SELECT 1 ->", row[0])

print("SMOKE OK")
dbutils.notebook.exit("SMOKE OK")
