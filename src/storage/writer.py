trigger: none

schedules:
- cron: "0 5 4 * *"
  displayName: Monthly S3 File Validation
  branches:
    include:
    - main
  always: true

pool:
  vmImage: ubuntu-latest

variables:
  AWS_REGION: 'ap-south-1'
  S3_BUCKET: 's3-kmpl-uat-01'
  S3_PREFIX: 'Welcome_Letter/ConvertedPDFs'
  FILE_PREFIX: 'customer_report'
  SMTP_SERVER: '10.240.20.136'
  EMAIL_TO: 'sourav.singh2@kotak.com,saumya.nair1@kotak.com,shruti.vyas@kotak.com'
  EMAIL_FROM: 'Aravind.e2@kotak.com'

steps:

# =========================================================
# STEP 1 : CHECK FILE EXISTS
# =========================================================

- task: AWSShellScript@1
  displayName: 'Check File Exists in S3'
  inputs:
    awsCredentials: 'DWH-UAT-Admin'
    regionName: '$(AWS_REGION)'
    scriptType: 'inline'
    inlineScript: |

      set -e

      DATE=$(date +"%Y%m")
      FILE_NAME="${FILE_PREFIX}_${DATE}.csv"

      echo "Expected File: $FILE_NAME"

      FILE_EXISTS=$(aws s3 ls s3://$(S3_BUCKET)/$(S3_PREFIX)/$FILE_NAME || true)

      if [ -z "$FILE_EXISTS" ]; then

          echo "File not found in S3"

          echo "Expected file $FILE_NAME not received on 4th of month." | mailx \
            -s "FAILURE: S3 File Missing" \
            -r "$(EMAIL_FROM)" \
            -S smtp="$(SMTP_SERVER)" \
            "$(EMAIL_TO)"

          exit 1

      fi

      echo "File found"

# =========================================================
# STEP 2 : DOWNLOAD FILE
# =========================================================

- task: S3Download@1
  displayName: 'Download S3 File'
  inputs:
    awsCredentials: 'DWH-UAT-Admin'
    regionName: '$(AWS_REGION)'
    bucketName: '$(S3_BUCKET)'
    sourceFolder: '$(S3_PREFIX)'
    globExpressions: '$(FILE_PREFIX)_*.csv'
    targetFolder: '$(System.DefaultWorkingDirectory)/downloads'

# =========================================================
# STEP 3 : INSTALL PYTHON LIBRARIES
# =========================================================

- task: Bash@3
  displayName: 'Install Python Dependencies'
  inputs:
    targetType: 'inline'
    script: |

      pip install pandas psycopg2-binary

# =========================================================
# STEP 4 : CALCULATE METRICS + PUSH TO REDSHIFT
# =========================================================

- task: PythonScript@0
  displayName: 'Calculate Metrics and Push to Redshift'
  inputs:
    scriptSource: 'inline'
    script: |

      import os
      import glob
      import pandas as pd
      import psycopg2

      DOWNLOAD_PATH = "$(System.DefaultWorkingDirectory)/downloads"

      files = glob.glob(f"{DOWNLOAD_PATH}/*.csv")

      if not files:
          raise Exception("No CSV files found")

      latest_file = files[0]

      print(f"Processing File: {latest_file}")

      df = pd.read_csv(latest_file)

      total_count = len(df)

      distinct_count = df['customer_id'].nunique()

      null_count = df['customer_id'].isnull().sum()

      print("Total Count:", total_count)
      print("Distinct Count:", distinct_count)
      print("Null Count:", null_count)

      conn = psycopg2.connect(
          host="your-redshift-host",
          port="5439",
          database="your_db",
          user="your_user",
          password="your_password"
      )

      conn.autocommit = True

      cursor = conn.cursor()

      create_table_sql = """
      CREATE TABLE IF NOT EXISTS file_metrics (
          file_name VARCHAR(500),
          run_date DATE,
          total_count BIGINT,
          distinct_count BIGINT,
          null_count BIGINT
      );
      """

      cursor.execute(create_table_sql)

      file_name = os.path.basename(latest_file)

      insert_sql = f"""
      INSERT INTO file_metrics
      VALUES (
          '{file_name}',
          CURRENT_DATE,
          {total_count},
          {distinct_count},
          {null_count}
      );
      """

      cursor.execute(insert_sql)

      print("Metrics inserted into Redshift")

      cursor.close()
      conn.close()

# =========================================================
# STEP 5 : COMPARE WITH PREVIOUS MONTH
# =========================================================

- task: PythonScript@0
  displayName: 'Compare Current vs Previous Metrics'
  inputs:
    scriptSource: 'inline'
    script: |

      import psycopg2

      conn = psycopg2.connect(
          host="your-redshift-host",
          port="5439",
          database="your_db",
          user="your_user",
          password="your_password"
      )

      cursor = conn.cursor()

      query = """

      SELECT
          total_count,
          distinct_count,
          null_count
      FROM file_metrics
      ORDER BY run_date DESC
      LIMIT 2

      """

      cursor.execute(query)

      rows = cursor.fetchall()

      if len(rows) < 2:
          print("Not enough historical data")
          exit(0)

      current = rows[0]
      previous = rows[1]

      current_total = current[0]
      previous_total = previous[0]

      current_distinct = current[1]
      previous_distinct = previous[1]

      current_null = current[2]
      previous_null = previous[2]

      def deviation(curr, prev):

          if prev == 0:
              return 0

          return abs(curr - prev) / prev * 100

      total_dev = deviation(current_total, previous_total)

      distinct_dev = deviation(current_distinct, previous_distinct)

      null_dev = deviation(current_null, previous_null)

      print("Total Deviation:", total_dev)
      print("Distinct Deviation:", distinct_dev)
      print("Null Deviation:", null_dev)

      alert_required = (
          total_dev > 5 or
          distinct_dev > 5 or
          null_dev > 5
      )

      if alert_required:

          with open("alert.txt", "w") as f:

              f.write(f'''
      ALERT: DATA QUALITY DEVIATION DETECTED

      Total Count Deviation    : {total_dev:.2f}%
      Distinct Count Deviation : {distinct_dev:.2f}%
      Null Count Deviation     : {null_dev:.2f}%

      Current Total    : {current_total}
      Previous Total   : {previous_total}

      Current Distinct : {current_distinct}
      Previous Distinct: {previous_distinct}

      Current Null     : {current_null}
      Previous Null    : {previous_null}
      ''')

      cursor.close()
      conn.close()

# =========================================================
# STEP 6 : SEND ALERT MAIL IF COUNTS OFF
# =========================================================

- task: Bash@3
  displayName: 'Send Threshold Alert Mail'
  condition: succeededOrFailed()
  inputs:
    targetType: 'inline'
    script: |

      if [ -f alert.txt ]; then

          cat alert.txt | mailx \
            -s "ALERT: File Metrics Deviated Beyond 5%" \
            -r "$(EMAIL_FROM)" \
            -S smtp="$(SMTP_SERVER)" \
            "$(EMAIL_TO)"

          echo "Alert Mail Sent"

      else

          echo "No deviation detected"

      fi

# =========================================================
# STEP 7 : SUCCESS MAIL
# =========================================================

- task: Bash@3
  displayName: 'Send Success Mail'
  condition: succeeded()
  inputs:
    targetType: 'inline'
    script: |

      echo "S3 file validation completed successfully." | mailx \
        -s "SUCCESS: S3 Validation Completed" \
        -r "$(EMAIL_FROM)" \
        -S smtp="$(SMTP_SERVER)" \
        "$(EMAIL_TO)"

# =========================================================
# STEP 8 : FAILURE MAIL
# =========================================================

- task: Bash@3
  displayName: 'Send Failure Mail'
  condition: failed()
  inputs:
    targetType: 'inline'
    script: |

      echo "Azure DevOps Pipeline Failed." | mailx \
        -s "FAILURE: S3 Validation Pipeline Failed" \
        -r "$(EMAIL_FROM)" \
        -S smtp="$(SMTP_SERVER)" \
        "$(EMAIL_TO)"
