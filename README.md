# Storage Gateway Performance Test
The purpose of this script is to measure the performance of AWS Storage Gateway. With this script, you can upload an entire folder structure to a Storage Gateway mount point on one thread, and a separate thread will poll S3 to measure how long it takes for each file to be available.

# Usage
1. Install a Storage Gateway and create a file share

2. Mount a path to the file share

3. Clone this repo and install the requirements
````
$ pip install -r requirements
````
4. Run the script to copy your file structure to the Storage Gateway
When the script completes, you can see per-file performance in `report.csv`
````
$ python ./sg-speed-test.py ~/source_path /mnt/storage_gateway storage_gateway_bucket
Upload DONE
Verification done
Verify DONE
Upload DONE
File Count: 100
Total Size: 242.87 MB
Execution Time: 8.721878 second(s)
Performance: 27.85 MB/sec

