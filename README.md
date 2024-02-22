# ToyPorject_Datawarehouse(car)
자동차 데이터 Datawarehouse 구축

1. 데이터 수집
  수집처 - 네이버 뉴스(자동차), 커뮤니티(보배드림)
  수집 주기 - Daily

2. 수집 데이터 저장
   저장소 - AWS S3
   파일 형식 - Parquet(Spark 데이터 처리를 위해 Parquet형식 사용)

3. 데이터 처리
  Spark를 활용해 PostgresqlDB로 저장
  
4. 처리 데이터 저장
   저장소 - Postgresql
   spark로 처리한 결과를 Postgresql로 저장하여 Datawarehouse 구축
