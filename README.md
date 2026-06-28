# Big Data Assignment
Το παρόν repository περιέχει όλα τα αρχεία κώδικα της εξαμηνιαίας εργασία Διαχείρισης Δεδομένων Μεγάλης Κλίμακας της φοιτήτριας με ΑΜ 03400293.

### Excecution scripts

#### Ζητούμενο 1
Quer1 με DF:
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

$ spark-submit code/prDFQ1.py --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

Δημιουργία αρχείων parquet:
spark-submit code/csv_to_parquet.py --output-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293/data_parquet

deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

$ spark-submit code/prDFQ1_par.py --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293 

#### Ζητούμενο 2
Query 1 με DF:
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.executor.memory=2g code/prDFQ1.py --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

Query 1 με DF με udf:
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

$ spark-submit   --conf spark.executor.instances=2   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prDFQ1
_udf.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

Query 1 με Rdds:
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=2   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prRddQ1.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

####  Ζητούμενο 3
Query 2 με DF:
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=4   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prDFQ2.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

Query 2 με SQL:
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=4   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prSQLQ2.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

#### Ζητουμενο 4
Q3 με DF
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=3   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prDFQ3.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

Q3 με Rdd
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=3   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prRddQ3.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

#### Ζητούμενο 5
Α. 2 executors
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=2   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prDFQ4.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=2   --conf spark.executor.cores=2   --conf spark.executor.memory=4g   code/prDFQ4.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=2   --conf spark.executor.cores=4   --conf spark.executor.memory=8g   code/prDFQ4.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

B. 8cores, 16gb
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=2   --conf spark.executor.cores=4   --conf spark.executor.memory=8g   code/prDFQ4.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=4   --conf spark.executor.cores=2   --conf spark.executor.memory=4g   code/prDFQ4.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=8   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prDFQ4.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293

#### Ζητούμενο 6
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=3   --conf spark.executor.cores=1   --conf spark.executor.memory=2g   code/prDFQ3_log.py   --base-path hdfs://hdfs-namenode.
default.svc.cluster.local:9000/user/dsml00293

deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit

spark-submit   --conf spark.executor.instances=4   --conf spark.executor.cores=2   --conf spark.executor.memory=4g   code/prDFQ4_log.py   --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00293
