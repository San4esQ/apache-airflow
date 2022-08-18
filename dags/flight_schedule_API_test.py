from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests

default_args = {
    'owner': 'san4es',
    'start_date': datetime(2022, 8, 10),
    'provide_context': True
}

arriving = 'https://api.rasp.yandex.net/v3.0/schedule/?apikey=0f423e2c-87a8-49e8-84d6-6d5710c7920b&station=TJM&date=2022-08-08&transport_types=plane&direction=all&event=arrival&system=iata'


# depart = 'https://api.rasp.yandex.net/v3.0/schedule/?apikey=0f423e2c-87a8-49e8-84d6-6d5710c7920b&station=TJM&date=2022-08-08&transport_types=plane&direction=all&system=iata '


def get_data_arriving(**kwargs):
    # инициализируем переменную ti
    ti = kwargs['ti']
    response = requests.get(arriving)
    if response.status_code == 200:
        data_arriving = response.json()

        # сохраняем/ загружаем файл  json передавая ключ key и value=сам файл json
        ti.xcom_push(key='flight_schedule_arriving_json', value=data_arriving)


def edit_data_arriving(**kwargs):
    ti = kwargs['ti']

    # достаем/извлекаем/получаем json через xcom из 'get_data_arriving' указывая key 'flight_schedule_arriving_json'
    flight_schedule_arriving_json = ti.xcom_pull(task_ids='get_data_arriving', key='flight_schedule_arriving_json')
    arrival_list = []
    route_name_list = []
    number_plane_list = []
    vehicle_list = []
    company_name_list = []
    # получаю ключ к списку schedule
    schedule = flight_schedule_arriving_json['schedule']

    for index in range(len(schedule)):
        # получаем время и дату
        search_arrival = schedule[index]['arrival']

        # делаю так потому что datetime через json 'Object of type datetime is not JSON serializable', без datetime БД высчитывает дату и результат добавляет в бд
        # а мог бы к примеру date = datetime.strftime(search_arrival[0:19], "%Y-%m-%dT%H:%M:%S")
        date_arriving = search_arrival[0:10]
        arrival_list.append(date_arriving)

        # получаем маршруты
        search_route_name = schedule[index]['thread']['title']
        route_name_list.append(search_route_name)

        # получаем список номеров самолетов
        search_number_plane = schedule[index]['thread']['number']
        number_plane_list.append(search_number_plane)

        # получаем список всех названий  самолетов
        search_vehicle = schedule[index]['thread']['vehicle']
        vehicle_list.append(search_vehicle)

        # получаем список всех названий компаний самолетов
        search_company_name = schedule[index]['thread']['carrier']['title']
        company_name_list.append(search_company_name)

    result_arriving = {'arrival': arrival_list,
                       'route_name': route_name_list,
                       'number_plane': number_plane_list,
                       'vehicle': vehicle_list,
                       'company_name': company_name_list}
    # возвращает словарь , где содержатся выбранные данные из апи по ключам
    ti.xcom_push(key='arriving_edit', value=result_arriving)


with DAG(
        dag_id='flight_schedule_v01',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,

) as dag:
    # передаем таску функцию get_data_arriving , где указываем в python_callable что он выполняет функцию get_data_arriving
    get_data_arriving = PythonOperator(task_id='get_data_arriving', python_callable=get_data_arriving)

    # передаем таску функцию edit_data_arriving , где указываем в python_callable что он выполняет функцию edit_data_arriving
    edit_data_arriving = PythonOperator(task_id='edit_data_arriving', python_callable=edit_data_arriving)

    # load_data_arriving = PythonOperator(task_id='load_data', python_callable=load_data)
    # создаем таблицу для нужных данных полученных из get_data_arriving и отредактированных таском edit_data_arriving
    # arrival-дата и время прибытия, route_name - названия маршрута, company_name - название компании самолета,
    # vehicle - название самолета, number_plane - номер самолета
    create_table_for_edit_data = PostgresOperator(
        task_id="create_table_for_edit_data",
        postgres_conn_id="PostgreSQL",
        sql="""
                                        CREATE TABLE IF NOT EXISTS flight_schedule (
                                        arrival TEXT,
                                        route_name  TEXT,
                                        number_plane TEXT,
                                        vehicle TEXT,
                                        company_name TEXT)
                                    """,
    )
    insert_edit_data_in_table = PostgresOperator(
        task_id="insert_edit_data_in_table",
        postgres_conn_id="PostgreSQL",
        sql=[f"""INSERT INTO flight_schedule VALUES(
                                            
                                 {{{{ti.xcom_pull(key='arriving_edit', task_ids=['edit_data_arriving'])[0]['arrival'][{i}]}}}},
                                '{{{{ti.xcom_pull(key='arriving_edit', task_ids=['edit_data_arriving'])[0]['route_name'][{i}]}}}}',
                                '{{{{ti.xcom_pull(key='arriving_edit', task_ids=['edit_data_arriving'])[0]['number_plane'][{i}]}}}}',
                                '{{{{ti.xcom_pull(key='arriving_edit', task_ids=['edit_data_arriving'])[0]['vehicle'][{i}]}}}}',
                                '{{{{ti.xcom_pull(key='arriving_edit', task_ids=['edit_data_arriving'])[0]['company_name'][{i}]}}}}')
                                """ for i in range(38)]
    )

    get_data_arriving >> edit_data_arriving >> create_table_for_edit_data >> insert_edit_data_in_table
