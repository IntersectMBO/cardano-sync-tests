import logging
import os
from pathlib import Path

from git import Repo
from git.exc import GitCommandError

LOGGER = logging.getLogger(__name__)


def git_clone_iohk_repo(repo_name: str, repo_dir: Path, repo_branch: str) -> Repo:
    """Clones an IOHK repository and checks out a specific branch."""
    try:
        repo = Repo.clone_from(f"https://github.com/input-output-hk/{repo_name}.git", repo_dir)
        repo.git.checkout(repo_branch)
        LOGGER.info(
            f"Repository {repo_name} successfully cloned to {repo_dir} and branch "
            f"{repo_branch} checked out."
        )
    except GitCommandError:
        LOGGER.exception(f"Error cloning repository {repo_name}")
        raise
    else:
        return repo


def git_checkout(repo: Repo, rev: str) -> Repo:
    """Check out a specific revision in the given repository."""
    try:
        repo.git.checkout(rev)
        LOGGER.info(f"Checked out revision {rev} in repository: {repo.working_dir}")
    except GitCommandError:
        LOGGER.exception(f"Error checking out revision {rev} in repository {repo.working_dir}")
        raise
    else:
        return repo


def clone_repo(repo_name: str, repo_branch: str) -> str:
    """Clones a repository and checks out a specific branch."""
    location = os.path.join(os.getcwd(), repo_name)
    try:
        repo = Repo.clone_from(f"https://github.com/input-output-hk/{repo_name}.git", location)
        repo.git.checkout(repo_branch)
        LOGGER.info(
            f"Repository {repo_name} successfully cloned to {location} and branch "
            f"{repo_branch} checked out."
        )
    except GitCommandError:
        LOGGER.exception(f"Error cloning repository {repo_name} to {location}")
        raise
    else:
        return location
