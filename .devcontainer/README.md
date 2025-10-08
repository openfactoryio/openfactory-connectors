# 🐳 Development Container Setup (VS Code + Dev Containers)

This project supports a pre-configured [Dev Container](https://containers.dev/) that:

- ✅ Automatically installs Python 3.13 and development tools
- ✅ Sets up the required environment variables
- ✅ Installs and exposes the `openfactory-sdk` for managing local Kafka/ksqlDB instances

---

## 🚀 Getting Started

### 1. Prerequisites

* [Docker](https://www.docker.com/)
* [VS Code](https://code.visualstudio.com/)

---

### 2. Open in Dev Container

1. Open this repository in VS Code.
2. Press `F1`, then select:

```
Dev Containers: Reopen in Container
```

VS Code will build the container using [.devcontainer/devcontainer.json](../.devcontainer/devcontainer.json).

---

### 3. Start Kafka + ksqlDB (One-node Dev Stack)

> ⚠️ **Note:** Kafka and ksqlDB are *not started automatically*.
> You must run `spinup` before deploying assets.

```bash
spinup
```

This will:

* Launch a single-node Kafka broker and ksqlDB instance (via the `openfactory-sdk`)
* Export the required environment variables into your shell session

To stop and clean up:

```bash
teardown
```

---

### 4. Deploy your Connector

Refer to the `README.md` of the OpenFactory Connector you work on.


> ⚠️ **Kafka Warnings Are Normal**
>
> During deployment you may see messages like:
>
> ```text
> %3|1753376340.630|FAIL|rdkafka#producer-1| [thrd:broker:29092/bootstrap]: broker:29092/bootstrap: Failed to resolve 'broker:29092': No address associated with hostname (after 1ms in state CONNECT)
> ```
>
> These are expected — Kafka is attempting to connect to internal broker hostnames that are not yet resolvable. It will automatically retry and connect once the cluster is ready.

---

## 🧪 Available Features

| Feature                   | Description                          |
| ------------------------- | ------------------------------------ |
| Python 3.13               | Pre-installed in the container       |
| `ofa`                     | CLI tool for asset management        |
| Kafka + ksqlDB (via SDK)  | One-node development setup           |
| VS Code Extensions        | Python + Docker tooling              |
| Dev Environment Variables | Set via `containerEnv` in the config |

---

## 📂 File Reference

Dev container configuration lives in:

```bash
.devcontainer/devcontainer.json
```

You can customize this file to add more packages, extensions, or tools as needed.

---

## 📌 Notes

* This is a **development-only environment** — not intended for production use.
* The `openfactory-sdk` version is pinned in the container configuration under `features` in [devcontainer.json](../.devcontainer/devcontainer.json). Update as needed.

---

## 🛠 Troubleshooting

* **Volume permission issues on Linux**: Ensure Docker is configured with correct user permissions. You may need to add your user to the `docker` group or adjust file system permissions.
* **Container doesn’t start?** Make sure Docker Desktop is running and WSL 2 is enabled (Windows).
* **Virtual asset doesn’t deploy?** After running `spinup`, wait a few minutes for all streams and tables to initialize before deploying assets.
