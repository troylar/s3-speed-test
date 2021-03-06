import boto3
import click
import threading
from multiprocessing import Process, Pipe
import os
from shutil import copyfile
import uuid
import datetime
import csv
import humanfriendly


client = boto3.client('s3')
uid = str(uuid.uuid4())


def does_key_exist(bucket, key):
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=key,
    )
    for obj in response.get('Contents', []):
        if obj['Key'] == key:
            return True


def verify(pipe, pipe_u, bucket):
    files = []
    upload_done = False
    while True:
        if pipe_u.poll():
            f = pipe_u.recv()
            if f and f == 'DONE':
                upload_done = True
            else:
                files.append(f)
        for s3_f in files:
            if does_key_exist(bucket, '{}/{}'.format(uid, s3_f)):
                files.remove(s3_f)
                pipe.send([s3_f, datetime.datetime.now()])
        if len(files) == 0 and upload_done:
            break
    print('Verification done')
    pipe.send('DONE')


def copy_file(source, target, pipe_v, pipe, relative_path, thread_limiter):
    thread_limiter.acquire()
    try:
        copyfile(source, target)
        end_time = datetime.datetime.now()
        copy_time = end_time
        size = os.path.getsize(source)
        pipe_v.send(relative_path)
        pipe.send([relative_path, size, end_time, copy_time])
    finally:
        thread_limiter.release()


def upload(pipe, pipe_v, folder, target, thread_limiter, verify):
    start = datetime.datetime.now()
    threads = []
    for root, dirs, files in os.walk(folder, topdown=True):
        for name in files:
            source_file = os.path.join(root, name)
            relative_path = source_file[len(folder) + 1:]
            target_file = os.path.join(target, uid, relative_path)
            target_folder = os.path.dirname(target_file)
            if not os.path.isdir(target_folder):
                os.makedirs(target_folder)
            copy_t = threading.Thread(target=copy_file,
                                      args=(source_file, target_file, pipe_v,
                                            pipe, relative_path, thread_limiter))
            threads.append(copy_t)
            copy_t.start()
    for t in threads:
        t.join()
    total_secs = (datetime.datetime.now() - start).total_seconds()
    print('Upload DONE ({} seconds)'.format(total_secs))
    pipe.send('DONE')
    pipe_v.send('DONE')


def write_report(report, wait_for_verify):
    with open('report.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File', 'Size in Bytes', 'S3 Latency in Ms'])
        for k in report.keys():
            row = report[k]
            if wait_for_verify:
                writer.writerow([k, row['size'], row['s3_latency']])
            else:
                writer.writerow([k, row['size']])


def report(upload_pipe, verify_pipe, start_time, wait_for_verify):
    report = {}
    upload_done = False
    verify_done = not wait_for_verify
    while True:
        if wait_for_verify:
            v = verify_pipe.recv()
            if v:
                if v == 'DONE':
                    print('Verify DONE')
                    verify_done = True
                else:
                    if v[0] not in report:
                        report[v[0]] = {}
                    report[v[0]]['s3_done_time'] = v[1]
        u = upload_pipe.recv()
        if u and u == 'DONE':
            print('Upload DONE')
            upload_done = True
        if verify_done and upload_done:
            break
        if u:
            if u[0] not in report:
                report[u[0]] = {}
            report[u[0]]['size'] = u[1]
            report[u[0]]['copy_done_time'] = u[2]
    total_size = 0
    for k in report:
        if wait_for_verify:
            report[k]['s3_latency'] = (report[k]['s3_done_time'] - report[k]['copy_done_time']).total_seconds() * 1000
        total_size = total_size + report[k]['size']

    write_report(report, wait_for_verify)
    time_in_secs = (datetime.datetime.now() - start_time).total_seconds()
    print('Total Size: {}'.format(humanfriendly.format_size(total_size)))
    print('File Count: {}'.format(len(report)))
    print('Execution Time: {} second(s)'.format(time_in_secs))
    print('Performance: {}/sec'.format(humanfriendly.format_size((total_size / time_in_secs))))


@click.command()
@click.argument('source_path')
@click.argument('target_path')
@click.argument('storage_gateway_bucket_name')
@click.option('--max_threads', default=100, help='Maximum number of threads')
@click.option('--wait_for_verify/--no_wait_for_verify', default=True, help='Wait for verify after copy')
@click.option('--output_file', default='./report.csv', help='Output report file')
def main(source_path, target_path, storage_gateway_bucket_name, output_file, max_threads, wait_for_verify):
    thread_limiter = threading.BoundedSemaphore(max_threads)
    start = datetime.datetime.now()
    upload_pipe_v, verify_pipe_v = Pipe()
    verify_pipe, report_v_pipe = Pipe()
    upload_pipe, report_u_pipe = Pipe()

    upload_p = Process(target=upload, args=(upload_pipe, upload_pipe_v, source_path, target_path, thread_limiter, wait_for_verify))
    if wait_for_verify:
        verify_p = Process(target=verify, args=(verify_pipe, verify_pipe_v, storage_gateway_bucket_name))
        verify_p.start()
    report_p = Process(target=report, args=(report_u_pipe, report_v_pipe, start, wait_for_verify))

    report_p.start()
    upload_p.start()
    upload_p.join()
    if wait_for_verify:
        verify_p.join()
    report_p.join()


if __name__ == '__main__':
    main()
