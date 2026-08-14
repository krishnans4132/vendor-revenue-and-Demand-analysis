# Vendor Revenue Analysis using Hadoop MapReduce
### NYC Yellow Taxi Trip Records (Feb 2023) — macOS Implementation Guide

This guide walks through your individual task end-to-end: dataset understanding →
macOS/Hadoop setup → data preparation → MapReduce implementation → execution →
validation → report content → viva prep.

Companion files in this project:
- `src/com/taxianalysis/vendorrevenue/VendorRevenueMapper.java`
- `src/com/taxianalysis/vendorrevenue/VendorRevenueReducer.java`
- `src/com/taxianalysis/vendorrevenue/VendorRevenueDriver.java`
- `data/convert_to_csv.py`
- `validation/validate_with_pandas.py`

---

## 1. Understanding the dataset

The **NYC Yellow Taxi Trip Records** dataset (published by the NYC Taxi & Limousine
Commission, TLC) contains one row per taxi trip, with pickup/dropoff times, locations,
fares, and payment details. The Feb 2023 file has roughly 3 million rows — comfortably
above the "5,000+ records" requirement in your project instructions.

**Full column list (2023 schema):**

| Column | Meaning |
|---|---|
| `VendorID` | Code for the technology/trip-record provider (see note below) |
| `tpep_pickup_datetime` / `tpep_dropoff_datetime` | Trip start/end timestamps |
| `passenger_count` | Number of passengers |
| `trip_distance` | Distance in miles |
| `RatecodeID` | Rate type (standard, JFK, Newark, negotiated, etc.) |
| `store_and_fwd_flag` | Whether the trip record was held in vehicle memory before sending |
| `PULocationID` / `DOLocationID` | Pickup/dropoff TLC taxi zone IDs |
| `payment_type` | Cash, credit card, etc. |
| `fare_amount` | Metered fare only |
| `extra` | Rush hour/overnight surcharges |
| `mta_tax` | Fixed MTA tax |
| `tip_amount` | Tip (only reliably captured for credit-card payments) |
| `tolls_amount` | Tolls paid |
| `improvement_surcharge` | Fixed surcharge |
| `total_amount` | **Sum of all the above** — total charged to the passenger |
| `congestion_surcharge` | NYC congestion pricing surcharge |
| `airport_fee` | Airport pickup fee |

**Important correction on "vendor":** `VendorID` does **not** identify a taxi company,
fleet, or driver. It identifies which of two authorized trip-record technology
providers supplied the data for that trip:
- `1` = Creative Mobile Technologies, LLC (CMT)
- `2` = Curb Mobility, LLC (formerly VeriFone Inc.)

So "vendor revenue" in this project means *total fare revenue processed through each
technology provider's system*, not the earnings of separate taxi companies. This is
worth stating explicitly in your report — it's a natural viva question and shows you
understood the data rather than just running code on it.

**Revenue column to use:** `total_amount`, not `fare_amount`. `fare_amount` is only the
metered fare; `total_amount` is fare + extras + MTA tax + tip + tolls + surcharges +
airport fee — i.e., the actual total money that changed hands for the trip. Two
caveats worth a sentence in your report: (1) `tip_amount` is only reliably recorded for
credit-card payments, so `total_amount` slightly understates true tips for cash fares;
(2) a very small number of rows have negative or zero `total_amount` (refunds/voided
trips/data errors) and should be excluded, not summed as negative revenue.

**Data-cleaning issues to handle:**
- Negative or zero `total_amount` — exclude (refunds, cancellations, corrupted rows)
- Missing/null `VendorID` or `total_amount` — exclude
- `VendorID` values outside the documented set — flag but don't necessarily drop (TLC
  has occasionally added new provider codes; check what appears in this file)
- Extreme outliers (e.g., `total_amount` in the thousands from data entry errors) — you
  can mention these exist but they don't need to be filtered for a *sum* analysis the
  same way they would for an *average* analysis
- Dropoff timestamp before pickup timestamp — not relevant to revenue, but worth
  mentioning as a general data-quality note

---

## 2. Problem statement

**Title:** Vendor Revenue Analysis using Hadoop MapReduce on NYC Yellow Taxi Trip Data

**Objective:** To compute the total trip revenue (`total_amount`) processed through
each taxi trip-record vendor (`VendorID`) in the NYC Yellow Taxi dataset, using a
distributed Hadoop MapReduce program, and to validate the result independently.

**Input:** `yellow_tripdata_2023-02.parquet` (NYC TLC Yellow Taxi trip records, ~3M
rows), reduced to a two-column CSV (`VendorID`, `total_amount`) for MapReduce
processing.

**Processing:** A MapReduce job where the Mapper extracts `(VendorID, total_amount)`
pairs from each valid record, and the Reducer sums `total_amount` per `VendorID`
after the shuffle-and-sort phase groups records by key.

**Output:** One line per vendor: `VendorID <tab> total_revenue`.

**Expected result:** Two (or a small number of) lines showing the total revenue
processed by each vendor across all trips in the file — the actual numbers must come
from running the job on your downloaded dataset (see Section 12, do not fabricate
these).

**Business/analytical significance:** This kind of aggregation is the taxi-analytics
equivalent of "revenue by channel" reporting — it shows how trip volume and revenue
are distributed across the technology providers that serve NYC's yellow taxi fleet,
which the TLC and researchers use for regulatory and market-share analysis, and
demonstrates a MapReduce pattern (grouped sum) that generalizes to sales analysis,
category-wise statistics, and similar aggregation problems named in your course
rubric.

---

## 3. Hadoop setup on macOS (Homebrew, pseudo-distributed mode)

This uses Homebrew, the simplest reliable path on macOS, and sets up a single-node
**pseudo-distributed** cluster — real HDFS and real MapReduce execution (as your
rubric requires "Upload the dataset into HDFS"), just running on one machine.

### 3.1 Install Java

```bash
# Check what's already installed
/usr/libexec/java_home -V

# If you need to install one, Hadoop works best with Java 8 or 11 (avoid
# very new JDKs, which can trigger module-access warnings/errors with Hadoop)
brew install openjdk@11
```

### 3.2 Install Hadoop

```bash
brew update
brew install hadoop
```

Find where it installed (path differs between Apple Silicon and Intel Macs):

```bash
brew --prefix hadoop
# Apple Silicon example: /opt/homebrew/opt/hadoop
# Intel example:         /usr/local/opt/hadoop
```

### 3.3 Set environment variables

Add to `~/.zshrc` (default shell on modern macOS — use `~/.bash_profile` if you're on
bash):

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 11)"
export HADOOP_HOME="$(brew --prefix hadoop)/libexec"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export YARN_HOME="$HADOOP_HOME"
```

Then apply it:

```bash
source ~/.zshrc
java -version
hadoop version
```

### 3.4 Configure passwordless SSH to localhost

Pseudo-distributed mode starts NameNode/DataNode processes over SSH, even to your
own machine:

```bash
# System Settings → General → Sharing → turn ON "Remote Login"

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa      # press enter through prompts if key exists already, skip
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

ssh localhost    # should log in with no password; type 'exit' to leave
```

### 3.5 Edit Hadoop configuration files

All in `$HADOOP_CONF_DIR` (i.e. `$HADOOP_HOME/etc/hadoop`).

**`hadoop-env.sh`** — make sure this line is set (uncomment/add it):
```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 11)"
```

**`core-site.xml`** — inside `<configuration>...</configuration>`:
```xml
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://localhost:9000</value>
</property>
```

**`hdfs-site.xml`**:
```xml
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
<property>
  <name>dfs.namenode.name.dir</name>
  <value>file:///Users/YOUR_USERNAME/hadoopdata/namenode</value>
</property>
<property>
  <name>dfs.datanode.data.dir</name>
  <value>file:///Users/YOUR_USERNAME/hadoopdata/datanode</value>
</property>
```
(Replace `YOUR_USERNAME`; `dfs.replication = 1` is correct for a single-node cluster —
no other DataNode exists to replicate to.)

**`mapred-site.xml`**:
```xml
<property>
  <name>mapreduce.framework.name</name>
  <value>yarn</value>
</property>
```

**`yarn-site.xml`**:
```xml
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle</value>
</property>
```

### 3.6 Format the NameNode (once, before first use)

```bash
hdfs namenode -format
```

### 3.7 Start HDFS and YARN

```bash
start-dfs.sh
start-yarn.sh
```

### 3.8 Verify Hadoop is working

```bash
jps
# Should list: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager

hdfs dfs -mkdir /test
hdfs dfs -ls /
```

Also check the web UIs in a browser:
- NameNode / HDFS health: http://localhost:9870
- ResourceManager / YARN jobs: http://localhost:8088

When you're done working, shut it down cleanly with `stop-yarn.sh` and `stop-dfs.sh`.

---

## 4. Handling the Parquet dataset

Plain Hadoop MapReduce (`TextInputFormat`) reads line-delimited text; it cannot parse
Parquet's binary columnar format without adding `parquet-hadoop`/`parquet-avro` to the
classpath and writing a custom `InputFormat`. That's real extra complexity for a
beginner project and doesn't change the MapReduce logic itself — so the practical
approach is to convert once, outside Hadoop, using Python.

**Yes, Python/pandas/pyarrow is needed for this one conversion step** — this does not
turn the project into a Spark project; Spark is a different distributed processing
engine entirely. Here, Python is only doing a one-time, single-machine file-format
conversion before Hadoop MapReduce does the actual distributed aggregation.

We only pull the two columns MapReduce needs (`VendorID`, `total_amount`), not the
full ~19-column dataset — pyarrow's columnar format makes this fast.

```bash
cd data/
pip3 install pandas pyarrow --break-system-packages   # or use a venv, see below
python3 convert_to_csv.py
```

Recommended: use a virtual environment instead of `--break-system-packages`:
```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas pyarrow
python3 convert_to_csv.py
deactivate
```

This produces `data/vendor_revenue_input.csv` with a header row and two columns:
```
VendorID,total_amount
1,15.3
2,9.8
...
```

---

## 5. MapReduce logic

```
Taxi CSV record (VendorID,total_amount)
        │
        ▼
      Mapper
   input key   : LongWritable (byte offset in file — unused)
   input value : Text (one CSV line)
   parses      : split(",") → VendorID, total_amount
   emits       : (Text VendorID, DoubleWritable total_amount)
        │
        ▼
  Shuffle & Sort
   Hadoop groups all values by key (VendorID) across all Mappers
        │
        ▼
     Reducer
   input  : (Text VendorID, Iterable<DoubleWritable> amounts)
   sums   : all amounts for that VendorID
   emits  : (Text VendorID, DoubleWritable totalRevenue)
```

**Mapper details** (see `VendorRevenueMapper.java`):
- Input format: default `TextInputFormat` — one call to `map()` per line
- Skips the header line and blank lines
- Skips malformed lines (wrong field count, non-numeric amount)
- Skips non-positive `total_amount` (refunds/errors)
- Emits `(VendorID, total_amount)` for every valid record
- Uses Hadoop `Counters` to track how many records were valid vs. skipped, for each
  reason — visible in the job's final console summary, useful evidence for your report

**Reducer details** (see `VendorRevenueReducer.java`):
- Receives all `total_amount` values already grouped by `VendorID`
- Sums them with a simple loop
- Rounds to 2 decimal places (currency)
- Also used as the **Combiner** — safe because addition is associative/commutative, so
  pre-summing on each Mapper's node before the network shuffle produces the same final
  result while cutting the amount of data transferred

**Example flow:**
```
Input lines:
  1,15.30
  2,9.80
  1,22.10

Mapper output:
  (1, 15.30)
  (2, 9.80)
  (1, 22.10)

After shuffle & sort:
  1 → [15.30, 22.10]
  2 → [9.80]

Reducer output:
  1    37.40
  2    9.80
```

---

## 6. Java implementation

Complete, runnable code is provided in three files:
- `src/com/taxianalysis/vendorrevenue/VendorRevenueMapper.java`
- `src/com/taxianalysis/vendorrevenue/VendorRevenueReducer.java`
- `src/com/taxianalysis/vendorrevenue/VendorRevenueDriver.java`

They use the standard `org.apache.hadoop.mapreduce.*` (new) API, `Text`/`DoubleWritable`
types, explicit malformed-record handling, and no hardcoded dataset-specific values
(paths and column positions are the only fixed assumptions, matching the CSV format
produced in Step 4).

---

## 7. Project directory structure

```text
vendor-revenue-analysis/
├── data/
│   ├── yellow_tripdata_2023-02.parquet     # downloaded from Kaggle (you add this)
│   ├── convert_to_csv.py                    # parquet -> csv conversion
│   └── vendor_revenue_input.csv             # generated by convert_to_csv.py
├── src/
│   └── com/taxianalysis/vendorrevenue/
│       ├── VendorRevenueMapper.java
│       ├── VendorRevenueReducer.java
│       └── VendorRevenueDriver.java
├── build/                                    # compiled .class files (created when you compile)
├── output/                                   # local copy of the HDFS result (for your report)
├── validation/
│   └── validate_with_pandas.py
├── PROJECT_GUIDE.md                          # this file
└── VendorRevenueAnalysis.jar                 # created in Step 8
```

No `lib/` folder of manually-downloaded JARs is needed — Homebrew's Hadoop already has
everything on `hadoop classpath`.

---

## 8. Compile and create the JAR

```bash
cd vendor-revenue-analysis

mkdir -p build

javac -classpath $(hadoop classpath) \
  -d build \
  src/com/taxianalysis/vendorrevenue/*.java

jar -cvf VendorRevenueAnalysis.jar -C build .

# Verify the JAR contains your compiled classes
jar tf VendorRevenueAnalysis.jar
```

You should see `com/taxianalysis/vendorrevenue/VendorRevenueMapper.class`, `...Reducer.class`,
and `...Driver.class` listed.

---

## 9. Upload the data to HDFS

```bash
hdfs dfs -mkdir -p /user/$(whoami)/vendor-revenue/input

hdfs dfs -put data/vendor_revenue_input.csv /user/$(whoami)/vendor-revenue/input/

# Verify the upload
hdfs dfs -ls /user/$(whoami)/vendor-revenue/input/

# Peek at the first few lines directly from HDFS
hdfs dfs -cat /user/$(whoami)/vendor-revenue/input/vendor_revenue_input.csv | head -5
```

What each command does: `-mkdir -p` creates the HDFS directory (and any missing parent
directories); `-put` copies the local file into HDFS (splitting it into blocks across
DataNodes — just one here); `-ls` lists directory contents to confirm the file arrived;
`-cat` streams file contents from HDFS to your terminal to sanity-check it.

---

## 10. Run the MapReduce job

```bash
hadoop jar VendorRevenueAnalysis.jar \
  com.taxianalysis.vendorrevenue.VendorRevenueDriver \
  /user/$(whoami)/vendor-revenue/input \
  /user/$(whoami)/vendor-revenue/output
```

- **Input path**: `/user/$(whoami)/vendor-revenue/input` — the HDFS directory
  containing `vendor_revenue_input.csv`
- **Output path**: `/user/$(whoami)/vendor-revenue/output` — must **not already
  exist**; Hadoop always creates it fresh
- **Mapper**: `VendorRevenueMapper`, set via `job.setMapperClass(...)` in the Driver
- **Reducer**: `VendorRevenueReducer`, set via `job.setReducerClass(...)`

During execution you'll see console output tracking Map % and Reduce % progress,
followed by a summary of Hadoop's built-in counters plus your custom
`RecordCounters` (valid/skipped records) — this is worth a screenshot for your report.

**Handling "Output directory already exists":** Hadoop refuses to overwrite an
existing output directory to avoid silently destroying previous results. Fix by either
removing the old output before rerunning:
```bash
hdfs dfs -rm -r /user/$(whoami)/vendor-revenue/output
```
or using a fresh, timestamped output path each run (handy for keeping a history while
you're testing):
```bash
hadoop jar VendorRevenueAnalysis.jar \
  com.taxianalysis.vendorrevenue.VendorRevenueDriver \
  /user/$(whoami)/vendor-revenue/input \
  /user/$(whoami)/vendor-revenue/output_$(date +%s)
```

---

## 11. View the results

```bash
hdfs dfs -ls /user/$(whoami)/vendor-revenue/output

hdfs dfs -cat /user/$(whoami)/vendor-revenue/output/part-r-00000

# Copy the result to your local machine for the report
mkdir -p output
hdfs dfs -get /user/$(whoami)/vendor-revenue/output/part-r-00000 output/vendor_revenue_result.txt
```

Expected output format (tab-separated `VendorID`, `total_revenue` — actual figures
come from your run, don't assume numbers ahead of time):

```text
1    XXXXXXX.XX
2    XXXXXXX.XX
```

(You may also see a `_SUCCESS` marker file alongside `part-r-00000` — that's Hadoop's
way of confirming the job finished cleanly; it's normal and not part of your data.)

---

## 12. Validation

Run `validation/validate_with_pandas.py` to independently compute the same totals
with pandas, as ground truth:

```bash
cd validation
python3 validate_with_pandas.py
```

Compare each number to the corresponding line in `output/vendor_revenue_result.txt`.
They should match closely — usually to the cent. If they differ:
- Check the Mapper's job counters (how many records were skipped, and why) against
  the row counts dropped in `convert_to_csv.py` — they should tell the same story
- Make sure both the CSV-conversion filter and the pandas validation filter use the
  same `total_amount > 0` rule
- A difference of a few cents across millions of floating-point additions is normal
  rounding, not a bug

---

## 13. Performance analysis

**Why MapReduce suits this problem:** grouped summation over millions of independent
rows is "embarrassingly parallel" at the Map stage — every record can be processed
without knowledge of any other record, which is exactly the workload MapReduce is
built for.

**Mapper parallelism:** Hadoop splits the input file into blocks (128MB by default)
and runs one Mapper task per split, in parallel across however many
Map slots/containers are available (on a real cluster, across many machines; here,
across your machine's cores).

**Shuffle & Sort:** After mapping, Hadoop partitions the `(VendorID, amount)` pairs by
key (default: hash of the key mod number of reducers), transfers each partition to its
assigned Reducer, and sorts each partition by key — so a Reducer receives all values
for a given `VendorID` already grouped together.

**Reducer aggregation:** Each Reducer sums the values for its group of keys — since
there are only a handful of distinct `VendorID` values, a single Reducer easily
handles this stage; the Combiner (Section 5) has already done most of the summation
work locally on each Mapper node, so shuffle traffic stays small.

**Time/space:** Map phase is O(n) and fully parallelizable across splits; sort is
O(n log n) per partition but partitions are small here since there are few distinct
keys; Reduce phase is O(v) output rows where v = number of distinct vendors (2–7),
so the final aggregation itself is trivial — the cost is dominated by reading and
mapping the input.

**Scaling to larger datasets:** The same job, unchanged, would process a full year of
NYC taxi data (tens of millions of rows across 12 monthly files) simply by pointing
the input path at a directory containing all the files, and by running on a real
multi-node cluster instead of one machine — more DataNodes store more blocks, more
NodeManagers run more Mapper/Reducer containers in parallel, and total runtime grows
much more slowly than linearly with data size because of that added parallelism.

---

## 14. Project report content

These map onto the "Document Requirements" section of your course instructions
(items 1–13 and 17 are yours; the Hive items 14–16 belong to your teammate's part of
the report):

- **Introduction** — Big Data Analytics on real-world transportation data; why NYC
  taxi data is a good fit (public, large, real, well-documented)
- **Problem Statement** — Section 2 above
- **Motivation** — understanding revenue distribution across trip-record providers is
  a realistic aggregation/reporting task, and demonstrates the classic MapReduce
  grouped-sum pattern named in your rubric ("Sales Analysis" / "Category-wise
  Statistics")
- **Dataset (with source link)** —
  https://www.kaggle.com/datasets/psvishnu/nyc-yellow-taxi-trip-records/?select=yellow_tripdata_2023-02.parquet,
  ~3M rows, describe each column per Section 1's table
- **Architecture** — the pipeline diagram in the "Architecture & plan" section
- **MapReduce Problem** — compute total `total_amount` grouped by `VendorID`
- **Mapper Logic Explanation** — Section 5
- **MapReduce Source Code** — paste `VendorRevenueMapper.java` and `VendorRevenueDriver.java`
- **Reducer Logic** — Section 5
- **Reducer Code** — paste `VendorRevenueReducer.java`
- **Input and Output** — sample input CSV rows and the actual output you get after
  running the job (Section 11)
- **Screenshots of execution** — terminal output from Step 10 (Map/Reduce progress,
  counters)
- **Output screenshots** — terminal output from Step 11 (`hdfs dfs -cat` result)
- **Conclusion** — summarize the revenue split you found, note the `VendorID`
  correction from Section 1, and mention validation agreement from Section 12

---

## 15. Presentation / viva preparation

**2–3 minute explanation of your contribution:**

"My part of the project is Vendor Revenue Analysis on the NYC Yellow Taxi dataset
using Hadoop MapReduce. I took the raw Parquet trip-record file, extracted just the
VendorID and total_amount columns with pandas, and loaded that into HDFS. My
MapReduce job's Mapper reads each trip record, validates it, and emits a
(VendorID, revenue) pair; the shuffle-and-sort phase groups all revenue values by
vendor; and my Reducer sums them into one total-revenue figure per vendor. I used a
Combiner to pre-aggregate on each Mapper node before the network shuffle, which is a
standard MapReduce optimization for associative operations like sum. I validated the
Hadoop result against an independent pandas calculation on the same data, and they
matched. The result shows how trip revenue is split between the two authorized
taxi-technology providers in the dataset."

**Workflow explanation for your professor:**
Use the diagram in Section 5 — Taxi CSV record → Mapper (parse + validate + emit
key/value) → Combiner (local partial sum) → Shuffle & Sort (group by VendorID) →
Reducer (final sum) → HDFS output.

**Likely viva questions and answers:**

1. *Why Hadoop MapReduce instead of Pandas?* — Pandas loads everything into a single
   machine's memory; MapReduce is designed to scale the same code across a cluster
   for datasets too large for one machine, distributing the work across many Mapper
   and Reducer tasks running in parallel.
2. *Why Hadoop MapReduce instead of Spark?* — The assignment specifically requires
   MapReduce; MapReduce also has a simpler execution model (explicit map/shuffle/
   reduce phases as compared to Spark's in-memory DAG execution) which is well
   suited to demonstrating the fundamentals.
3. *What does the Mapper do?* — Parses each CSV line, validates it, and emits
   (VendorID, total_amount).
4. *What does the Reducer do?* — Sums all total_amount values grouped under one
   VendorID.
5. *What is Shuffle & Sort?* — The phase between Map and Reduce where Hadoop
   partitions Mapper output by key, transfers each partition to the right Reducer,
   and sorts it so all values for a key arrive together.
6. *What is a Combiner and why did you use one?* — A local mini-reducer that runs on
   the Mapper's output before the shuffle; safe here because sum is associative, and
   it reduces network traffic by pre-aggregating.
7. *What is HDFS and why upload the data there?* — Hadoop's distributed file system;
   MapReduce jobs read their input from and write their output to HDFS so that data
   is split into blocks and available near where Map/Reduce tasks execute.
8. *Why convert Parquet to CSV first?* — Plain MapReduce's TextInputFormat reads
   line-delimited text, not Parquet's binary columnar format; converting once with
   pandas/pyarrow avoids needing extra parquet-hadoop dependencies while keeping the
   MapReduce logic itself unchanged.
9. *What does VendorID actually represent?* — The taxi-record technology provider
   (CMT or Curb Mobility/VeriFone) that supplied the trip data, not a taxi company
   or driver.
10. *Why total_amount instead of fare_amount as "revenue"?* — total_amount is the
    full amount charged (fare + extras + tolls + surcharges + tip), i.e. the actual
    total money collected for the trip, while fare_amount is only the metered fare.
11. *How did you handle bad/missing data?* — The Mapper skips the header, blank
    lines, malformed rows, and non-positive total_amount values, and tracks each
    case with a Hadoop Counter.
12. *How did you validate your result?* — Compared it against an independent pandas
    groupby().sum() on the same cleaned data.
13. *What happens if you rerun the job with the same output path?* — Hadoop throws
    "Output directory already exists" and fails, by design, to avoid overwriting
    prior results; you must delete or rename the output path first.
14. *How would this scale to a full year of taxi data?* — Point the input path at a
    directory of all monthly files and run on a real multi-node cluster; the same
    unchanged Mapper/Reducer code parallelizes across more machines.
15. *What's the time complexity of your job?* — Map phase is O(n) and parallel
    across input splits; the sort is O(n log n) per partition; Reduce is O(v) output
    rows for v distinct vendors, since there are very few distinct keys here.

---

## Complete end-to-end checklist

- [ ] Download `yellow_tripdata_2023-02.parquet` from Kaggle into `data/`
- [ ] Install Java 11 and Hadoop via Homebrew (Section 3.1–3.2)
- [ ] Set environment variables in `~/.zshrc` and `source` it (Section 3.3)
- [ ] Enable Remote Login and confirm passwordless `ssh localhost` (Section 3.4)
- [ ] Edit `hadoop-env.sh`, `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`,
      `yarn-site.xml` (Section 3.5)
- [ ] `hdfs namenode -format` (once)
- [ ] `start-dfs.sh` and `start-yarn.sh`; confirm with `jps` and the web UIs
      (Section 3.7–3.8)
- [ ] Install pandas/pyarrow and run `convert_to_csv.py` to produce
      `vendor_revenue_input.csv` (Section 4)
- [ ] Compile the Java code and build `VendorRevenueAnalysis.jar` (Section 8)
- [ ] Create the HDFS input directory and `hdfs dfs -put` the CSV (Section 9)
- [ ] Run the job with `hadoop jar ...` (Section 10)
- [ ] View and save the output with `hdfs dfs -cat` / `-get` (Section 11)
- [ ] Run `validate_with_pandas.py` and confirm the totals match (Section 12)
- [ ] Take screenshots: job execution progress, counters, and final output
- [ ] Write up the report sections (Section 14)
- [ ] Rehearse the 2–3 minute summary and viva Q&A (Section 15)
- [ ] `stop-yarn.sh` and `stop-dfs.sh` when finished for the day
