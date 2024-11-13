# Data Engineering Project(Data Pipelie)

데이터 엔지니어링 프로젝트는 자동차와 관련된 데이터를 수집합니다.
기존에 사용하던 프로젝트의 기술과는 다른 기술을 사용하기 위해 시작되었으며,
다양한 오픈소스 프로젝트를 사용하여 구축되었습니다.

## 아키텍처 다이어그램 

![Toy_projcet_diagram](https://github.com/user-attachments/assets/901f1bfc-48e2-4217-a711-9b864c62d5b8)


## 작동 원리

### 데이터 수집

Airflow DAG는 Python(Selenium, Bs4)을 통해 수집 모듈의 실행합니다.
매일 주기적으로 실행되어 배치를 생성합니다.


### 데이터 흐름

- Python을 통해 수집처에서 데이터를 수집하여 AWS s3에 Parquet형태로 저장합니다.
이때, Parquet형태로 저장하는 이유는 추후 Spark가 데이터를 읽기 쉽게 하기 위해서입니다.

- Parquet형태의 데이터는 Spark를 통해 전처리 됩니다.
전처리된 데이터는 데이터 프레임 형태로 postgreSQL로 저장됩니다.

- 저장된 데이터는 추후 DataWarehouse의 역할을 할 수 있습니다.
