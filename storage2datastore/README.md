# Cloud Storage To Cloud Datastore Sample

Cloud Storageにあるファイルを読み込み、加工を行い、Cloud Datastoreに書き込むサンプル

## Run

```
mvn compile exec:java -Dexec.mainClass=org.sinmetal.beam.examples.storage2datastore.StorageToDatastore -Dexec.args="--runner=DataflowRunner --project=sinmetal-dataflow \
     --tempLocation=gs://input-sinmetal-dataflow/tmp \
     --inputFile=gs://input-sinmetal-dataflow/data.csv \
     --categoryMasterInputFile=gs://input-sinmetal-dataflow/category.csv" -Pdataflow-runner
```