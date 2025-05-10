#!/usr/bin/env bash
script_dir="$(dirname -- "$(realpath -- "$0")")"
workspace_dir="$(dirname "${script_dir}")"

# fix Git
if ! git config --global --get safe.directory >/dev/null; then
	git config --global --add safe.directory "${workspace_dir}"
fi

# set up pre-commit
pre-commit install --install-hooks
