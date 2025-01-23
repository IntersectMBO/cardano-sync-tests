import logging
import os

from git import Repo
from git.exc import GitCommandError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def git_clone_iohk_repo(repo_name, repo_dir, repo_branch):
    """Clones an IOHK repository and checks out a specific branch."""
    try:
        repo = Repo.clone_from(
            f"https://github.com/input-output-hk/{repo_name}.git", repo_dir
        )
        repo.git.checkout(repo_branch)
        logging.info(
            f"Repository {repo_name} successfully cloned to {repo_dir} and branch {repo_branch} checked out."
        )
        return repo
    except GitCommandError as e:
        logging.exception(f"Error cloning repository {repo_name}: {e}")
        raise


def git_checkout(repo, rev):
    """Checks out a specific revision in the given repository."""
    try:
        repo.git.checkout(rev)
        logging.info(f"Checked out revision {rev} in repository: {repo.working_dir}")
        return repo
    except GitCommandError as e:
        logging.exception(
            f"Error checking out revision {rev} in repository {repo.working_dir}: {e}"
        )
        raise


def clone_repo(repo_name, repo_branch):
    """Clones a repository and checks out a specific branch"""
    location = os.path.join(os.getcwd(), repo_name)
    try:
        repo = Repo.clone_from(
            f"https://github.com/input-output-hk/{repo_name}.git", location
        )
        repo.git.checkout(repo_branch)
        logging.info(
            f"Repository {repo_name} successfully cloned to {location} and branch {repo_branch} checked out."
        )
        return location
    except GitCommandError as e:
        logging.exception(f"Error cloning repository {repo_name} to {location}: {e}")
        raise
