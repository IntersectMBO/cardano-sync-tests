"""Git utilities for cloning and checking out Cardano repositories."""

from __future__ import annotations

import logging
import os
import pathlib as pl

from git import Repo
from git.exc import GitCommandError

from sync_tests.utils.configuration import CARDANO_GITHUB_ORG

LOGGER = logging.getLogger(__name__)


def _fetch_updates(repo: Repo) -> None:
    """Fetch remote branches and tags for an existing checkout.

    This keeps long-lived local clones fresh so newly published tags can be
    checked out by name (for example ``10.6.2``).
    """
    try:
        repo.git.fetch("--all", "--tags", "--prune")
    except GitCommandError:
        LOGGER.warning(
            "Failed to refresh refs/tags for repository %s",
            repo.working_dir,
            exc_info=True,
        )


def git_clone_iohk_repo(repo_name: str, repo_dir: pl.Path, repo_branch: str) -> Repo:
    """Clones a Cardano repository and checks out a specific branch."""
    try:
        repo = Repo.clone_from(f"https://github.com/{CARDANO_GITHUB_ORG}/{repo_name}.git", repo_dir)
        repo.git.checkout(repo_branch)
        LOGGER.info(
            "Repository %s successfully cloned to %s and branch %s checked out.",
            repo_name,
            repo_dir,
            repo_branch,
        )
    except GitCommandError:
        LOGGER.exception("Error cloning repository %s", repo_name)
        raise
    else:
        return repo


def git_checkout(repo: Repo, rev: str) -> Repo:
    """Check out a specific revision in the given repository."""
    _fetch_updates(repo)
    try:
        repo.git.checkout(rev)
        LOGGER.info("Checked out revision %s in repository: %s", rev, repo.working_dir)
    except GitCommandError:
        LOGGER.exception("Error checking out revision %s in repository %s", rev, repo.working_dir)
        raise
    else:
        return repo


def clone_repo(repo_name: str, repo_branch: str) -> str:
    """Clone a repository into the current working directory, or reuse it if already present."""
    location = os.path.join(os.getcwd(), repo_name)

    # Reuse an existing checkout when possible (idempotent behavior).
    if os.path.exists(location):
        try:
            repo = Repo(path=location)
        except Exception as e:
            msg = (
                f"Destination path '{location}' already exists but is not a git repository. "
                "Move/delete it and retry."
            )
            raise RuntimeError(msg) from e
        _fetch_updates(repo)
        git_checkout(repo, repo_branch)
        LOGGER.info(
            "Repository %s already present at %s; checked out revision %s.",
            repo_name,
            location,
            repo_branch,
        )
        return location

    try:
        repo = Repo.clone_from(f"https://github.com/{CARDANO_GITHUB_ORG}/{repo_name}.git", location)
        git_checkout(repo, repo_branch)
        LOGGER.info("Repository %s successfully cloned to %s.", repo_name, location)
    except GitCommandError:
        LOGGER.exception("Error cloning repository %s to %s", repo_name, location)
        raise
    else:
        return location
