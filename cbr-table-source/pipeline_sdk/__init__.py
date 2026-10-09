"""pipeline_sdk — helpers for custom pipeline nodes.

Storage v2: every write goes through the backend's upload-session API
(multipart-only, quota-enforced server-side). Reads use fresh presigned GET
URLs resolved by the backend at read time.

High-level (recommended) — `context` wraps the whole NODE_CONTEXT contract:

    from pipeline_sdk import context

    def handler(ctx, workdir):
        src = ctx.input().download(workdir)
        result = process(src, ctx.config)
        ctx.output("result").upload_file(result)

    context.run(handler)  # status JSON + exit code handled

Injected environment — `env` for typed access to every platform variable
(BACKEND_URL, optional API_KEY/AUTHZ_URL, bucket names; no storage credentials
are injected):

    from pipeline_sdk import env

    token = env.require_api_key()              # clear error, never a KeyError
    bucket = env.require_artifact_bucket()

Layering — each module only imports the ones to its left:

    util  ->  env  ->  storage  ->  context

`util` holds shared helpers (HTTP post with retry, errors, diagnostics) and
reads nothing from the environment. `env` is the only module that reads
environment variables. `storage` and `context` build on them.

Secrets — `env.get_secret("NAME")` returns a user-stored secret and registers
it for log masking: if the node later prints that value, the logs show
`[REDACTED]` instead (see `log_mask` for the limits).

Low-level — `storage` for direct reads/writes by path:

    from pipeline_sdk import storage

    storage.upload_file("results/model.bin", "/tmp/model.bin")
    # size known -> one multipart session sized by the SERVER (small file = 1 part)

    with storage.open_write("results/output.parquet") as f:  # size unknown
        for chunk in produce_data():
            f.write(chunk)
    # context exit -> completeUpload (server verifies real sizes and commits)

    data = storage.read("inputs/data.csv")

Older node code that talks to storage through `boto3.client("s3")` can switch
to `from pipeline_sdk import boto3_compat as boto3`: the same calls, routed
through `storage`.

Server errors are printed to stderr with a [pipeline_sdk] prefix (pod logs
are archived and shown in the execution UI) and raised as StorageError with
`.status` / `.detail` set.
"""

from . import context, env, storage, util

__all__ = ["context", "env", "storage", "util"]
