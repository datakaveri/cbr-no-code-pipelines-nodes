# pipelines-nodes

Custom pipeline nodes for the CBR data platform.

## Shared SDK (`cbr-pipeline-sdk`)

Every Python node uses the same SDK, published on PyPI as **`cbr-pipeline-sdk`**
(import name `pipeline_sdk`). Each node's Dockerfile installs a pinned version:

```dockerfile
RUN pip install --no-cache-dir cbr-pipeline-sdk==2.2.0
```

To pick up an SDK fix, bump that version in the Dockerfiles and rebuild the image.
Nothing is copied between node folders any more.

The modules, each importing only the ones above it:

| Module | Use it for |
|--------|------------|
| `util` | Shared HTTP helper with retries (used by `env` and `storage`) |
| `env` | The only module that reads platform environment variables: `env.node_context_raw()`, `env.api_key()`, `env.artifact_bucket()` / `env.require_artifact_bucket()`, `env.get_secret("NAME")` |
| `storage` | Reads and writes by path through the backend (`read`, `read_to_file`, `open_write`, `write_bytes`, `upload_file`) |
| `boto3_compat` | A boto3-shaped client for older node code (`from pipeline_sdk import boto3_compat as boto3`); every call goes through `storage` |

Node code does not read `NODE_CONTEXT`, `API_KEY` or the bucket variables from `os.environ`;
it asks `env`. (`auth-check` is the exception: it exists to inspect the raw environment.)

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
