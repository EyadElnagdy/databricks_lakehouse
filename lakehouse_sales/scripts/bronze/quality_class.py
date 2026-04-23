from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import uuid, json

class QualityRunner:
    def __init__(self, spark, layer: str, table_name: str, run_id=None, pipeline_run_id=None):
        self.spark = spark
        self.layer = layer
        self.table_name = table_name
        self.run_id  = run_id or str(uuid.uuid4()) #generates a random id
        self.run_ts = datetime.utcnow()
        self.pipeline_run = pipeline_run_id or "manual"
        self.results = []
        self.has_critical_failure = False

 #an internal function to not be called outside the class _ is a naming convension for that
    def _log(self, check_name, check_type, status, severity, records_total, records_failed, details: dict):
        failure_rate = records_failed / records_total if records_total > 0 else 0
        if status == "FAILED" and severity == "CRITICAL":
            self.has_critical_failure = True
        self.results.append({
            "run_id": self.run_id,
            "run_timestamp": self.run_ts,
            "layer": self.layer,
            "table_name": self.table_name,
            "check_name": check_name,
            "check_type": check_type,
            "status": status,
            "severity": severity,
            "records_total": records_total,
            "records_failed": records_failed,
            "failure_rate": failure_rate,
            "details": json.dumps(details),
            "pipeline_run_id": self.pipeline_run,
        })
    #a function to check the row count (to be called outside the class)
    def check_row_count(self, df: DataFrame, min_rows: int, severity="CRITICAL"):
            total = df.count() #counts the rows from the dataframe
            passed = total >= min_rows
            self._log(
                check_name = "row_count_min",
                check_type = "completeness",
                status = "PASSED" if passed else "FAILED",
                severity = severity,
                records_total = total,
                records_failed= 0 if passed else total,
                details = {"min_expected": min_rows, "actual": total}
            )
            return self #return self enables method chaining
    #a function to check the null values in specific columns with a threshold
    def check_nulls(self, df: DataFrame, columns: list, threshold: float = 0.0, severity="CRITICAL"):
        total = df.count()
        for col in columns:
            nulls = df.filter(F.col(col).isNull()).count()
            rate  = nulls / total if total > 0 else 0
            passed = rate <= threshold
            self._log(
                check_name    = f"null_check_{col}",
                check_type    = "completeness",
                status        = "PASSED" if passed else "FAILED",
                severity      = severity,
                records_total = total,
                records_failed= nulls,
                details       = {"column": col, "null_rate": rate, "threshold": threshold}
            )
        return self

    def check_duplicates(self, df: DataFrame, key_columns: list, severity="CRITICAL"):
        total = df.count()
        dupes = total - df.dropDuplicates(key_columns).count()
        self._log(
            check_name    = f"duplicate_check_{'_'.join(key_columns)}",
            check_type    = "uniqueness",
            status        = "PASSED" if dupes == 0 else "FAILED",
            severity      = severity,
            records_total = total,
            records_failed= dupes,
            details       = {"key_columns": key_columns, "duplicate_count": dupes}
        )
        return self

    def check_values_in_set(self, df: DataFrame, column: str, valid_values: list, severity="HIGH"):
        total  = df.count()
        failed = df.filter(~F.col(column).isin(valid_values)).count()
        self._log(
            check_name    = f"value_set_{column}",
            check_type    = "validity",
            status        = "PASSED" if failed == 0 else "FAILED",
            severity      = severity,
            records_total = total,
            records_failed= failed,
            details       = {"column": column, "valid_values": valid_values}
        )
        return self

    def check_referential_integrity(self, df: DataFrame, fk_col: str, ref_df: DataFrame, pk_col: str, severity="HIGH"):
        total  = df.count()
        failed = df.join(ref_df.select(pk_col), df[fk_col] == ref_df[pk_col], "left_anti").count()
        self._log(
            check_name    = f"ref_integrity_{fk_col}",
            check_type    = "consistency",
            status        = "PASSED" if failed == 0 else "FAILED",
            severity      = severity,
            records_total = total,
            records_failed= failed,
            details       = {"fk_column": fk_col, "pk_column": pk_col}
        )
        return self

    # ── Commit & gate ────────────────────────────────────────

    def commit(self, raise_on_critical: bool = True):
        """Writes results to the log table and optionally blocks the pipeline."""
        log_df = self.spark.createDataFrame(self.results)
        log_df.write.format("delta").mode("append").saveAsTable("sales.audit.quality_log")
        self._print_summary()
        if raise_on_critical and self.has_critical_failure:
            raise Exception(
                f"[QualityRunner] CRITICAL checks failed for {self.layer}.{self.table_name}. "
                "Pipeline halted. See audit.quality_log for details."
            )
        return self.results

    def _print_summary(self):
        passed = sum(1 for r in self.results if r["status"] == "PASSED")
        failed = sum(1 for r in self.results if r["status"] == "FAILED")
        print(f"\n{'='*55}")
        print(f"  Quality Report | {self.layer.upper()} | {self.table_name}")
        print(f"  Run ID: {self.run_id}")
        print(f"  ✅ Passed: {passed}  ❌ Failed: {failed}")
        print(f"{'='*55}\n")
    
