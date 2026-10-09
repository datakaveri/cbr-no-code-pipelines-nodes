# pipelines-nodes

Custom pipeline nodes for the CBR data platform.

## Shared SDK (`pipeline_sdk/`)

Every Python node uses the same SDK, `pipeline_sdk/`, instead of its own copy of a storage
helper. Layering, one direction only:

```
util  ->  env  ->  storage  ->  context        (boto3_compat sits on env + storage)
```

| Module | Use it for |
|--------|------------|
| `env` | The only module that reads platform environment variables: `env.node_context_raw()`, `env.api_key()`, `env.artifact_bucket()` / `env.require_artifact_bucket()`, `env.get_secret("NAME")` |
| `storage` | Reads and writes by path through the backend (`read`, `read_to_file`, `open_write`, `write_bytes`, `upload_file`) |
| `boto3_compat` | A boto3-shaped client for older node code (`from pipeline_sdk import boto3_compat as boto3`); every call goes through `storage` |
| `util` | Shared HTTP helper with retries (used by `env` and `storage`) |

Node code does not read `NODE_CONTEXT`, `API_KEY` or the bucket variables from `os.environ`;
it asks `env`. (`auth-check` is the exception: it exists to inspect the raw environment.)

A node's Docker build context is its own folder, so each node folder holds a copy of the SDK.
The master copy is `./pipeline_sdk`; never edit a node's copy by hand.

```bash
python3 scripts/sync_sdk.py           # copy the master into every node
python3 scripts/sync_sdk.py --check   # fail if any copy differs from the master
```

To use the SDK in a node, add `COPY pipeline_sdk/ ./pipeline_sdk/` to its Dockerfile and run the
sync script. The master is kept in step with `packages/pipeline-sdk-py` in
`datakaveri/cbr-no-code-editor`; change it there first, then copy it here.

## Setup

### Clone with submodules

```bash
git clone --recurse-submodules https://github.com/datakaveri/pipelines-nodes.git
```

If you already cloned without submodules, initialize them after:

```bash
git submodule update --init
```

### Submodules

| Path | Repo |
|------|------|
| `omop-wide-table-source/sdk-upstream` | https://github.com/datakaveri/cbr-data-access-ipynb-sdk.git |

### Updating submodule URL

If you ever change the URL in `.gitmodules`, always run `git submodule sync` after so that `.git/config` stays in sync:

```bash
# 1. Edit .gitmodules with the correct URL
# 2. Sync into .git/config
git submodule sync
# 3. Re-initialize if needed
git submodule update --init
```

### Adding a new submodule

Use `git submodule add` — it writes to both `.gitmodules` and `.git/config` in one step:

```bash
git submodule add https://github.com/datakaveri/<repo>.git path/to/dir
```
