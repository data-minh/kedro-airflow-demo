from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
import smtplib
from email.mime.text import MIMEText


def send_email_smtp():
    # Get Airflow Variable
    to_email = Variable.get("email_to")
    smtp_user = Variable.get("smtp_user")
    smtp_password = Variable.get("smtp_password")

    print("to_email: ", to_email)
    print("smtp_user: ", smtp_user)

    # Tạo nội dung email
    msg = MIMEText("Kedro pipeline đã hoàn thành thành công.")
    msg["Subject"] = "✅ Kedro DAG Success"
    msg["From"] = smtp_user
    msg["To"] = to_email

    # Gửi email qua Gmail SMTP
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


with DAG(
    dag_id="kedro_data_processing_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    kedro_run = BashOperator(
        task_id="run_kedro_pipeline",
        bash_command="""
        cd $AIRFLOW_HOME/my-first-project && \
        kedro run --env=local --pipeline=data_processing --runner=my_first_project.runners.NodeSkippingRunner
        """,
    )

    send_email_task = PythonOperator(
        task_id="send_email_notification",
        python_callable=send_email_smtp,
    )

    end_task = EmptyOperator(
        task_id="end_task"
    )

    start_task >> kedro_run >> send_email_task >> end_task