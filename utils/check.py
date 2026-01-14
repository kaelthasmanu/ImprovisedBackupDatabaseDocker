import hashlib

def file_sha256sum(filepath, chunk_size=8192):
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            sha256.update(chunk)
    return sha256.hexdigest()

def verify_file_integrity(filepath, expected_hash):
    return file_sha256sum(filepath) == expected_hash

def copy_file(src, dst):
    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        while True:
            chunk = fsrc.read(8192)
            if not chunk:
                break
            fdst.write(chunk)
