import click
from multiprocessing import Process, Pipe
import os

def verify(pipe):
    files = []
    while True:
        f = pipe.recv()
        if f == 'DONE':
            break
        files.append(f)
    print(files)

def upload(pipe, folder, target):
    for root, dirs, files in os.walk(folder, topdown=True):
       for name in files:
          path = os.path.join(root, name).strip(folder)
          pipe.send(path)
    pipe.send('DONE')

@click.command()
@click.option('--source', help='Folder to upload')
@click.option('--target', help='Target path')
def main(source, target):
    upload_pipe, verify_pipe = Pipe()
    upload_p = Process(target=upload, args=(upload_pipe, source, target))
    verify_p = Process(target=verify, args=(verify_pipe, ))
    upload_p.start()
    verify_p.start()

if __name__ == '__main__':
    main()
