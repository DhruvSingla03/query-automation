import shutil

def get_separator(char="="):
    try:
        width = shutil.get_terminal_size().columns
    except:
        width = 80
    return char * width
