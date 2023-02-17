import subprocess

def open_file(filename):

    try:
        subprocess.run(['open', '-a', 'Google Chrome', filename])
    except Exception as e:
        print('Err:', str(e))