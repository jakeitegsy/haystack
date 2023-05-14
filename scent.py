import sniffer.api
import subprocess
watch_paths = ['tests/', 'src/']

@sniffer.api.runnable
def run_tests(*args):
    if subprocess.run(
        'python -m unittest -f tests/*.py',
        shell=True
    ).returncode == 0:
        return True