import os


def setup_module():
    """Ensure the test environment variables are set"""
    try:
        with open('build/test-environment') as f:
            for line in f:
                line = line.removeprefix('export ')
                name, _, value = line.strip().partition('=')
                os.environ[name] = value
    except OSError:
        pass
