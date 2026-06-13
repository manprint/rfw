# Release workflow

Releases are published automatically when a Git tag matching `v<major>.<minor>.<patch>` is pushed.

## What the workflow does

1. Runs `build.sh` on GitHub Actions.
2. Collects every binary emitted into `dist/`.
3. Publishes a GitHub Release containing all generated binaries.
4. Builds and pushes the Docker image to `ghcr.io/manprint/rfw`.

## Published Docker tags

For each release tag, GitHub Container Registry receives three tags:

- `ghcr.io/manprint/rfw:<git-tag>`
- `ghcr.io/manprint/rfw:latest`
- `ghcr.io/manprint/rfw:<full-commit-sha>`

## Creating a release

```bash
git tag v1.0.1
git push origin v1.0.1
```

After the workflow completes, the binaries are available in the GitHub Release and the container image is available on GHCR.