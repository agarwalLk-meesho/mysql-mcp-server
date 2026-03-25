# mysql-mcp-server

MCP server for MySQL over **stdio**. Tools: `connect_mysql`, `disconnect`, `list_databases`, `list_tables`, `describe_table`, `run_query` (read-only SQL only).

## Use without cloning (recommended for teams)

### After the package is on PyPI (or your private index)

Install [uv](https://docs.astral.sh/uv/), then run the server with **`uvx`** (no repo checkout):

```bash
uvx mysql-mcp-server
```

Pin a version for reproducible MCP configs:

```bash
uvx mysql-mcp-server==0.1.0
```

**Cursor** — `.cursor/mcp.json`:

```json
"mysql": {
  "command": "uvx",
  "args": ["mysql-mcp-server"]
}
```

**Claude Code** — project `.mcp.json` or:

```bash
claude mcp add --transport stdio mysql -- uvx mysql-mcp-server
```

**Private index** (example):

```bash
UV_INDEX_URL=https://your-artifact-server/simple uvx mysql-mcp-server
```

Or set `UV_INDEX` / `PIP_INDEX_URL` per [uv docs](https://docs.astral.sh/uv/configuration/indexes/) for your org.

### Before PyPI: install from Git (no full monorepo clone)

`uvx` can install the package from a **Git URL** pointing at this subdirectory (replace `ORG`, `REPO`, and branch/tag):

```bash
uvx --from "git+https://github.com/ORG/REPO.git#subdirectory=mysql_server&branch=main" mysql-mcp-server
```

Use a **tag or commit SHA** instead of `branch=main` for stable installs.

---

## Install from this repo (developers)

```bash
cd mysql_server
uv sync
uv run mysql-mcp-server
```

---

## Publishing (maintainers)

So others can run `uvx mysql-mcp-server` from the default index, the package must be on **PyPI** (or a private index your team configures).

### Option A — GitHub Actions (recommended, no long-lived PyPI token on laptops)

1. **One-time on [pypi.org](https://pypi.org):** open project **mysql-mcp-server** (create it if needed) → **Settings → Publishing** → add a **trusted publisher** for this GitHub repo with workflow file **`publish-mysql-mcp-server.yml`** (see comments at the top of [`.github/workflows/publish-mysql-mcp-server.yml`](../.github/workflows/publish-mysql-mcp-server.yml)).
2. Bump **`version`** in [`pyproject.toml`](pyproject.toml) and merge to the default branch.
3. Trigger the workflow:
   - **GitHub → Actions → Publish mysql-mcp-server to PyPI → Run workflow**, or  
   - Push tag **`mysql-mcp-server-v0.1.0`** (prefix `mysql-mcp-server-v` + semver from `pyproject.toml`, e.g. version `0.1.0` → tag `mysql-mcp-server-v0.1.0`).
4. Confirm on [PyPI](https://pypi.org/project/mysql-mcp-server/).

### Option B — From your machine

1. Bump **`version`** in [`pyproject.toml`](pyproject.toml).
2. Remove stray `*.egg-info` under `src/` if present (`rm -rf src/*.egg-info`).
3. Build and upload:

   ```bash
   cd mysql_server
   uv build
   uv publish
   ```

   Configure credentials via [uv publish](https://docs.astral.sh/guides/publish/) (`UV_PUBLISH_TOKEN`, or `keyring` / `~/.pypirc`).

If the PyPI name is taken, change `name` in `pyproject.toml` and update all docs/commands.

---

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) on `PATH` (for `uvx` / `uv sync`)
- Network access to MySQL (VPN if internal)

### Installing `uv`

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or: brew install uv
uv --version
```

---

## Cursor (monorepo / editable install)

If you develop inside the repo:

```json
"mysql": {
  "command": "uv",
  "args": [
    "--directory",
    "/absolute/path/to/mysql-mcp-server/mysql_server",
    "run",
    "mysql-mcp-server"
  ]
}
```

**Legacy:** `.venv/bin/python3` + `server.py` shim is supported after `uv sync`.

---

## Claude Code (monorepo + `AI_BOT_ROOT`)

If your monorepo uses a root env var, point it at this package directory, for example:

```bash
export REPO_ROOT="/absolute/path/to/mysql-mcp-server"
claude mcp add --transport stdio mysql -- \
  uv --directory "$REPO_ROOT/mysql_server" run mysql-mcp-server
```

---

## Security

- Prefer **read-only** MySQL users.
- Use VPN for internal DB hosts.
- Never commit passwords; use `connect_mysql` in-session or your secret manager.
