import subprocess

try:
    filename = '../test.html'
    subprocess.run(['open', '-a', 'Google Chrome', filename])
except Exception as e:
    print('Err:', str(e))