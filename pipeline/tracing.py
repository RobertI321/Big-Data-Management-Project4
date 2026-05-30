# Track runs: run_id generation, sha256 fingerprint, git_sha capture

import uuid
import subprocess
import hashlib

def get_git_sha():
    """
    Capture current git commit id
    """
    git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"])
    git_sha = git_sha.decode().strip()

    return git_sha

def generate_run_id():

    return str(uuid.uuid4())

def fingerprint(data : bytes | str):
    """
    Goes on every row in screens_metadata, screens_embeddings & screens_review_queue
    as "source_fingerprint". It identifies the source file (e.g hash of PNG bytes etc) 
    so we can DETECT DUPLICATES ACROSS RUNS
    """
    if isinstance(data, str):
        data = data.encode()
    data_hashed = hashlib.sha256(data).hexdigest()
    
    return data_hashed