# Simple PySpark Example

## Files
- `people.csv` — small sample dataset
- `simple_etl.py` — PySpark script that:
  - reads the CSV
  - filters people with age >= 30
  - computes average salary and count per city
  - prints the result
  - saves output as CSV (`output/avg_salary_by_city_csv/`) and Parquet (`output/avg_salary_by_city_parquet/`)

## Run (macOS/Linux)
```bash
cd pyspark_simple
spark-submit simple_etl.py
# or interactively
pyspark
# then inside PySpark shell:
# spark.read.option("header", True).option("inferSchema", True).csv("people.csv").show()
```

## Run (Windows, Command Prompt)
```cmd
cd pyspark_simple
spark-submit simple_etl.py
```

## PySpark Shell (quick interactive)
```bash
pyspark
```
Then try:
```python
df = (spark.read.option("header", True).option("inferSchema", True).csv("people.csv"))
df.select("city", "salary").groupBy("city").avg("salary").show()
```

## Notes
- Ensure Java and Spark are installed and on your PATH.
- Output folders are created under `output/`.

