import email
import json
import pathlib
import pendulum
import requests
import requests.exceptions as request_exceptions
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#curl -L "https://ll.thespacedevs.com/2.0.0/launch/upcoming" -o /data/launches.json

#define the DAG
dag = DAG(
    dag_id="download_rocket_launches",
    start_date=pendulum.now("UTC").subtract(days=14),
    schedule="@daily", # 0 0 * * * 
)

#task to download launches.json from the space devs api
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag = dag,
    )

#function to download pictures from launches.json
def _get_pictures():
    #ensure directory exists 
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    #download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        images_urls = [launch["image"] for launch in launches["results"]]
        for image_url in images_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print("downloaded {image_url} to {target_file}")
            except request_exceptions.MissingSchema as e:
                print(f"Error occurred while fetching {image_url}: {e}")

            except request_exceptions.ConnectionError as e:
                print(f"Error occurred while fetching {image_url}: {e}")

#task to download pictures
get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

#notifies user of how many images were downloaded
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images | wc -l) images in /tmp/images"',
    dag=dag,
)

#dependecies
download_launches >> get_pictures >> notify
