# Twitter Data Processing with Hadoop & Spark



**[Download Data](https://figshare.com/ndownloader/files/38650880)**

---

## ⚙️ Local Preprocessing

Due to memory and CPU constraints on the local machine, preprocessing was done before using GCP:

- Merged multiple `.json` files into a single NDJSON file (`5m-data-ndjson.json`).
- Ensured proper formatting for stream-based processing.
- Uploaded this preprocessed file to Google Cloud Storage (GCS) for distributed processing.

---

## ☁️ GCP Setup & Execution

### Step 1: Create GCS Bucket and Dataproc Cluster

```bash
# Set project and region
gcloud config set project YOUR_PROJECT_ID
gcloud config set dataproc/region us-central1

# Create a GCS bucket
gsutil mb -l us-central1 gs://cse532-a4-bucket/

# Create a Dataproc cluster
gcloud dataproc clusters create cluster-a4 \
  --region=us-central1 \
  --zone=us-central1-a \
  --master-machine-type=n1-standard-2 \
  --worker-machine-type=n1-standard-2 \
  --num-workers=2
```

###  Step 2: Preprocess and Upload Data
```bash
# Assuming preprocessing is done locally
gsutil cp 5m-data-ndjson.json gs://cse532-a4-bucket/data/
```

Upload your code:

```bash
gsutil cp task1_spark.py gs://cse532-a4-bucket/scripts/
gsutil cp task1_hadoop.py gs://cse532-a4-bucket/scripts/
gsutil cp task1_nondistributed.py gs://cse532-a4-bucket/scripts/

gsutil cp task2_spark.py gs://cse532-a4-bucket/scripts/
gsutil cp task2_hadoop.py gs://cse532-a4-bucket/scripts/
gsutil cp task2_nondistributed.py gs://cse532-a4-bucket/scripts/

gsutil cp task3_spark.py gs://cse532-a4-bucket/scripts/
```
### Step 3: SSH into Master Node & Execute Code

```bash
# SSH into the cluster master
gcloud compute ssh cluster-a4-m --zone us-central1-a
```
Once inside:

```bash
# Create workspace
mkdir ~/a4_workspace
cd ~/a4_workspace

# Copy scripts from GCS
gsutil cp gs://cse532-a4-bucket/scripts/*.py .

# Run Spark Task 1
spark-submit task1_spark.py \
  gs://cse532-a4-bucket/data/5m-data-ndjson.json \
  gs://cse532-a4-bucket/output/task1_spark_output

# Run Hadoop Task 1
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
  -input gs://cse532-a4-bucket/data/5m-data-ndjson.json \
  -output gs://cse532-a4-bucket/output/task1_hadoop_output \
  -mapper "python3 task1_hadoop.py mapper" \
  -reducer "python3 task1_hadoop.py reducer" \
  -file task1_hadoop.py

# Run Non-distributed Task 1
python3 task1_nondistributed.py \
  gs://cse532-a4-bucket/data/5m-data-ndjson.json \
  gs://cse532-a4-bucket/output/task1_non_dist_output.txt
```
Repeat similar steps for Task 2 and Task 3 by running respective Spark, Hadoop, and Python scripts.

## 📊 Performance Evaluation

Execution time was measured using:
- /usr/bin/time -v for local tasks
- Spark logs and Dataproc metrics for GCP tasks
- Hadoop job logs (from command line summary)
All runtimes are recorded in performance.txt.

## 📁 Output
Final outputs are saved in:

- gs://cse532-a4-bucket/output/

Each task has a separate folder for Spark, Hadoop, and non-distributed outputs.
