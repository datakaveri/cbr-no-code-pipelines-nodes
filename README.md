# pipelines-nodes

Custom pipeline nodes for the CBR data platform.

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
