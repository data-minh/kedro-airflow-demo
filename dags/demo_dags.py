from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from datetime import datetime
import smtplib
from email.mime.text import MIMEText
import psycopg2


def send_email_with_node_info(**context):
    # Lấy thông tin từ Airflow Variables
    to_email = Variable.get("email_to")
    smtp_user = Variable.get("smtp_user")
    smtp_password = Variable.get("smtp_password")

    # Lấy run_id hiện tại
    run_id = context['dag_run'].run_id

    # Lấy thông tin kết nối PostgreSQL từ Airflow Connection (nên dùng)
    # conn = BaseHook.get_connection("my_postgres")  # Connection ID bạn đặt trong Airflow UI
    # pg_conn = psycopg2.connect(
    #     dbname=conn.schema,
    #     user=conn.login,
    #     password=conn.password,
    #     host=conn.host,
    #     port=conn.port,
    # )

    pg_conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user="airflow",
        password="airflow",
        dbname="kedro_logs",
    )

    # Truy vấn dữ liệu node theo run_id
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT name, status, start_time, finish_time, detail
        FROM nodes
        WHERE run_id = %s
        ORDER BY start_time
    """, (run_id,))
    rows = cursor.fetchall()

    # Tạo bảng HTML chứa thông tin node
    html_rows = "\n".join([
        f"""
        <tr>
            <td>{name}</td>
            <td>{status}</td>
            <td>{start}</td>
            <td>{finish or ''}</td>
            <td>{detail or ''}</td>
        </tr>
        """
        for name, status, start, finish, detail in rows
    ])
    html_table = f"""
        <html><body>
        <p><b>Kết quả thực thi DAG:</b> <code>{run_id}</code></p>
        <table border="1" cellpadding="5" cellspacing="0">
            <tr>
                <th>Node</th>
                <th>Status</th>
                <th>Start Time</th>
                <th>Finish Time</th>
                <th>Detail</th>
            </tr>
            {html_rows}
        </table>
        </body></html>
    """

    # Tạo email
    msg = MIMEText(html_table, "html")
    msg["Subject"] = f"✅ Kedro DAG Completed: {run_id}"
    msg["From"] = smtp_user
    msg["To"] = to_email

    # Gửi email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    cursor.close()
    pg_conn.close()



with DAG(
    dag_id="kedro_data_processing_dag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    kedro_run = BashOperator(
        task_id="run_kedro_pipeline",
        bash_command=f"""
        cd $AIRFLOW_HOME/demo_project && \
        kedro run --env=docker \
                  --pipeline=demo_pipeline \
                  --runner=demo_project.runners.NodeSkippingRunner
        """,
    )

    send_email_task = PythonOperator(
        task_id="send_email_notification",
        python_callable=send_email_with_node_info,
    )

    end_task = EmptyOperator(
        task_id="end_task"
    )

    start_task >> kedro_run >> send_email_task >> end_task