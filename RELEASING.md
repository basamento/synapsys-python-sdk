# Releasing

Releases are published to PyPI from GitHub Actions using trusted publishing. A
developer machine does not hold a PyPI token.

## One-time PyPI setup

Create the `basamento-synapsys` project (the first release can be uploaded manually
if PyPI requires it), then add a trusted publisher for:

- owner: `basamento`
- repository: `synapsys-python-sdk`
- workflow: `release.yml`
- environment: `pypi`

Protect the GitHub `pypi` environment if release approval is desired.

## Cut a release

1. Update `project.version` in `pyproject.toml` and `__version__` in
   `src/basamento_synapsys/__init__.py` to the same semantic version.
2. Commit the change to `main`.
3. Create and push an annotated `v<version>` tag.

```bash
git tag -a v0.2.0 -m "Release 0.2.0"
git push origin v0.2.0
```

The workflow verifies that the tag and both package versions agree, runs the full
quality suite, builds the wheel and source distribution, checks their metadata, and
publishes them through PyPI's short-lived trusted-publisher credentials.

Published PyPI versions are immutable. If validation fails before publication, fix
the commit and create a new tag; never attempt to replace an already published file.

