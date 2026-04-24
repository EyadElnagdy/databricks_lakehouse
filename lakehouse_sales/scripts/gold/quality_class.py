from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import uuid, json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class QualityRunner:
    def __init__(self, spark, layer, table_name=None,run_id=None, pipeline_run_id=None, alert_emails=None ,smtp_host="smtp.gmail.com", smtp_port=587,smtp_user="eyadelnagdy@gmail.com", smtp_password="pyvh mzbh hafb hvxf"):
        self.spark        = spark
        self.layer        = layer
        self.table_name   = table_name
        self.run_id       = run_id or str(uuid.uuid4())
        self.run_ts       = datetime.utcnow()
        self.pipeline_run = pipeline_run_id or self._get_pipeline_run_id()
        self.results      = []
        self.has_critical_failure = False
        # email config
        self.alert_emails  = alert_emails or []       # list of recipient emails
        self.smtp_host     = smtp_host
        self.smtp_port     = smtp_port
        self.smtp_user     = smtp_user
        self.smtp_password = smtp_password
    def _get_pipeline_run_id(self):
        """Tries to get the Databricks job run ID automatically, falls back to 'manual' if not in a job context."""
        try:
            context = json.loads(
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .toJson()
            )
            job_id     = context.get("tags", {}).get("jobId", "unknown_job")
            run_id     = context.get("tags", {}).get("multitaskParentRunId") \
                    or context.get("tags", {}).get("idInJob", "unknown_run")
            return f"job_{job_id}_run_{run_id}"
        except Exception:
            return "manual"

    def _send_alert_emails(self):
        """Sends a warning email to all recipients in self.alert_emails if there are failures."""

        # collect only failed checks
        failures = [r for r in self.results if r["status"] == "FAILED"]
        if not failures or not self.alert_emails:
            return   # nothing to send
        
        # build the email body as an HTML table
        rows = ""
        for f in failures:
            rows += f"""
                <tr>
                    <td>{f['table_name']}</td>
                    <td>{f['check_name']}</td>
                    <td>{f['check_type']}</td>
                    <td style="color:red"><b>{f['status']}</b></td>
                    <td>{f['severity']}</td>
                    <td>{f['records_failed']} / {f['records_total']}</td>
                    <td>{f['details']}</td>
                </tr>
            """

        html = f"""
        <html><body>
            <h2>⚠️ Quality Check Failures Detected</h2>
            <p>
                <b>Layer:</b> {self.layer.upper()}<br>
                <b>Run ID:</b> {self.run_id}<br>
                <b>Timestamp:</b> {self.run_ts}<br>
                <b>Pipeline Run:</b> {self.pipeline_run}
            </p>
            <table border="1" cellpadding="6" cellspacing="0">
                <thead>
                    <tr>
                        <th>table</th>
                        <th>Check</th>
                        <th>Type</th>
                        <th>Status</th>
                        <th>Severity</th>
                        <th>Failed / Total</th>
                        <th>Details</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </body></html>
        """

        # send to each recipient
        for recipient in self.alert_emails:
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = f"[Quality Alert] {self.layer.upper()} | {self.table_name} | {len(failures)} failure(s)"
                msg["From"]= self.smtp_user
                msg["To"]= recipient
                msg.attach(MIMEText(html, "html"))

                with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
                    server.sendmail(self.smtp_user, recipient, msg.as_string())

                print(f"  📧 Alert sent to {recipient}")

            except Exception as e:
                # never let email failure crash the pipeline
                print(f"  ⚠️ Failed to send alert to {recipient}: {e}")

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
    def check_row_count(self, df: DataFrame,table_name, min_rows: int, severity="CRITICAL"):
            self.table_name=table_name
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
    def check_nulls(self, df: DataFrame,table_name, columns: list, threshold: float = 0.0, severity="CRITICAL"):
        self.table_name=table_name
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

    def check_duplicates(self, df: DataFrame,table_name, key_columns: list, severity="CRITICAL"):
        self.table_name=table_name
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

    def check_values_in_set(self, df: DataFrame,table_name, column: str, valid_values: list, severity="HIGH"):
        self.table_name=table_name
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

    def check_referential_integrity(self, df: DataFrame,table_name, fk_col: str, ref_df: DataFrame, pk_col: str, severity="HIGH"):
        self.table_name=table_name
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

    def commit(self, raise_on_critical=True):
        log_df = self.spark.createDataFrame(self.results)
        log_df.write.format("delta").mode("append").saveAsTable("sales.audit.quality_log")
        self._print_summary()
        self._send_alert_emails()
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
    
