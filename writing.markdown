## 통계 테이블을 통한 데이터 추출 요청 처리

---
### 개요
스타트업 데이터 팀에서 일하다보면 타 부서의 데이터 추출 요청을 정말 많이 받게됩니다.  
저희 회사의 경우 최근에 빅쿼리(BigQuery)를 이용한 데이터 웨어하우징 시스템을 도입했습니다. 빅쿼리는 구글에서 제공하고 일반적인 데이터베이스 시스템보다 훨씬 빠른 쿼리 처리 속도를 자랑하는 데이터 저장 및 분석용 클라우드 서비스입니다. 그래서 웬만한 데이터 추출 요청은 무리없이 처리할 수 있게 되었습니다.  
하지만, 간혹 ~~끔찍한~~ 까다로운 요청이 들어올 때가 있습니다. 그런 까다로운 추출 요청을 데이터 엔지니어링과 결합해 풀어나가는 한 가지 방법을 소개하려고 합니다. 

### 예시
오늘의 요청 예시는 아래와 같습니다.
- (일별, 주별, 월별 조회 시) 모든 등록 상품의 누적판매가 0건인 호스트의 수
- (일별, 주별, 월별 조회 시) 모든 등록 상품의 누적판매가 0건인 호스트 중 판매전환된 호스트의 수  

사실 요청 내용만 놓고 보면 그리 복잡하지 않습니다. 저희 서비스는 판매자(host)가 입점해서 구매자(customer)와 만나는 형태의 플랫폼인데, 요청 내용을 정리하자면 입점은 했지만 아직 상품을 한번도 팔지 못한 호스트의 수를 일별로 또는 주별, 월별로 추출해 변화의 추이를 보고싶다는 것입니다.  
그런데 위 요청을 처리하는 과정은 일반적인 데이터 추출 요청보다 까다롭습니다. 이유인 즉슨, 요청한 정보는 '일별 누적'인데 데이터베이스에는 해당 내용에 대한 누적 통계를 저장하고 있지 않기 때문입니다.  

### 기존 테이블을 이용한 해결방안
위 요청을 기존 테이블에 대한 SQL로 처리하는 과정은 아래와 같습니다. (본 글에서는 일별로만 추출하였습니다)
```sql
with date_raw as (
  select dt Date_Ranges
  from unnest(generate_date_array('2021-10-01', '2021-10-05', interval 1 day)) dt
)
, purchase_raw as (
  select hostId, paid -- paid: 결제 완료 시점
  from `purchase`
)
, host_raw as (
  select id, date(created) created
  from `host`
)
, date_host_raw as (
  select d.*, h.id
  from date_raw d
  cross join host_raw h
)
, date_join_host_raw as (
  select distinct  r.*
  from date_host_raw r
  left join host_raw h on r.id=h.id and r.Date_Ranges >= h.created
  where h.id is not null
  order by id
)
, date_purchase_raw as (
  select d.*, h.hostId
  from date_raw d
  cross join purchase_raw h
)
, date_join_purchase_raw as (
  select distinct r.*, date(p.paid) as paid
  from date_purchase_raw r
  left join purchase_raw p on r.hostId=p.hostId and r.Date_Ranges >= p.paid
  where p.hostId is not null
)
, unsold_host_raw as (
  select d.Date_Ranges, d.id as hostId
  from date_join_host_raw d
  left join date_join_purchase_raw p on d.Date_Ranges=p.Date_Ranges and d.id=p.hostId
  where p.hostId is null
)
, purchase_converted_host as (
  select Date_Ranges, count(distinct hostId) as conversion_cnt 
  from (
    -- 누적판매 0건인 호스트 중 다음날 결제가 발생한 호스트
    select (Date_Ranges+1) as Date_Ranges, h.hostId, p.hostId as p_host, paid
    from unsold_host_raw h
    left join purchase_raw p on h.hostId = p.hostId and h.Date_Ranges+1 = date(paid)
    where p.hostId is not null
  )
  group by Date_Ranges
)
, unsold_host_cnt as (
  select Date_Ranges as date, count(distinct hostId) host_cnt 
  from unsold_host_raw
  group by 1
)

select u.date, host_cnt, p.conversion_cnt
from unsold_host_cnt u
left join purchase_converted_host p on u.date = p.Date_Ranges
order by 1
```

위 SQL에서는 데이터 추출을 위해 조회하고자 하는 날짜 테이블과의 **크로스 조인**을 이용해 인위적으로 일자별 누적 테이블을 만들어 처리해주었습니다. 
크로스 조인을 진행한 테이블과 원본 호스트 테이블을 left join하는 과정에서 `r.Date_Ranges >= h.created`라는 조건을 걸어줘 각 호스트가 존재했던 모든 날짜에 매핑된 테이블 `date_join_host_raw`가 완성됩니다.  

비슷한 방법으로 설정한 기간 이전에 한번이라도 결제가 발생한 호스트를 추려내는 테이블 `date_join_purchase_raw`을 만들었습니다.  

두 테이블을 조인하면 비로소 일자별로 group by한 후 통계를 낼 수 있는 `unsold_host_raw` 테이블이 만들어집니다.

위 쿼리를 실행하면 아래 테이블과 같은 결과가 나옵니다(물론 회사의 실데이터는 소중하기 때문에 숫자는 가짜입니다ㅎㅎ).

|date|host_cnt|conversion_cnt|
|:---:|:---:|:---:|
|2021-10-01|518|*null*|
|2021-10-02|519|1|
|2021-10-03|528|1|
|2021-10-04|533|1|
|2021-10-05|539|2|

테이블 내용: '2021-10-02'에는 가입 이후 한번도 결제가 발생하지 않은 호스트가 519명이 있었고, 그 중 한명이 다음날인 '2021-10-03'에 첫 판매 전환을 하였습니다. 다른 쿼리를 이용해 확인해보면 '2021-10-03'에는 총 10명의 신규가입 호스트가 있습니다. 그래서 '2021-10-03'의 host_cnt는 총 528명이 됩니다. 

기존 테이블을 이용한 방식의 문제는 크로스 조인을 사용한다는 점입니다. 크로스 조인은 테이블을 엄청나게 뻥튀기해 처리하는 방식이기 때문에 컴퓨팅 파워도 많이 들고 한번 처리할 때 시간이 오래 걸리기 때문입니다. 위 예시에서는 5일 동안의 통계를 추출했지만, 조회기간이 30일 이상 되면 처리 시간이 분단위로 넘어갑니다: 저희 회사의 경우 4~5분 정도가 소요되었습니다.

### 통계 테이블을 이용한 해결방안
이와 같은 문제를 해결하는 한 가지 방법은 **매일 필요한 정보를 저장해 통계 테이블**을 만드는 것입니다. 저 같은 경우 빅쿼리에서 제공하는 schedule query를 이용하는데, 매일 특정 시간에 저장된 쿼리를 실행함으로써 통계 테이블에 값을 저장할 수 있습니다. 하지만 꼭 빅쿼리를 사용하지 않더라도 airflow 또는 AWS lambda + cloudwatch 등 주기적으로 실행되는 코드만 작성할 수 있다면 얼마든지 적용 가능합니다.

저는 아래와 같은 테이블(stats_table)을 설계해 사용했습니다. 

|hostId|productId|cummulative_purchase_cnt|job_date|
|:---:|:---:|:---:|:---:|
|1|1|0|2021-10-01|
|1|2|0|2021-10-01|
|1|3|0|2021-10-01|
|2|4|12|2021-10-01|
|2|5|4|2021-10-01|
|3|6|0|2021-10-01|
|1|1|0|2021-10-02|
|1|2|1|2021-10-02|
|1|3|0|2021-10-02|
|2|4|12|2021-10-02|
|2|5|4|2021-10-02|
|3|6|0|2021-10-02|

위 통계 테이블을 이용해 처음 SQL로 해결한 방식과 똑같은 결과를 추출하려면 아래와 같이 쿼리를 짜면 됩니다.

```sql
with host_purchase_stat as (
  select job_date, hostId, sum(cummulative_purchase_cnt) purchase_cnt
  from stats_table  -- 통계 테이블
  group by 1, 2
)
, unsold_host as (
  select job_date, hostId, purchase_cnt
  from host_purchase_stat
  where purchase_cnt = 0
)
, purchased_host as (
  select job_date, hostId, purchase_cnt
  from host_purchase_stat
  where purchase_cnt > 0
)
, converted_host_cnt as (
  select job_date, count(hostId) conversion_cnt
  from (
    select p.job_date, p.hostId
    from unsold_host u
    left join purchased_host p on u.hostId = p.hostId and date(u.job_date) + 1 = date(p.job_date)
    where p.hostId is not null
  )
  group by job_date
)

select t1.job_date, host_cnt, conversion_cnt
from (
  select job_date, count(*) host_cnt 
  from unsold_host 
  group by 1
) t1
left join converted_host_cnt t2 on t1.job_date = t2.job_date
``` 

처음 쿼리와 비교해봤을 때 로직도 간단해지고 속도도 비교할 수 없을 정도로 빨라졌습니다. 물론 주기적으로 데이터를 적재하기 위한 관리포인트가 추가되었지만, 해당 데이터를 앞으로도 꾸준히 사용할 예정이라면 통계 테이블을 이용하는 것의 장점이 더 많다고 생각합니다. 왜냐하면 간단한 코드를 이용해 일별 매출 등의 메타데이터를 추가로 저장한다면 더 다양한 분석을 위한 테이블로 확장시킬 수도 있기 때문입니다.  

통계 테이블 방식의 가장 큰 단점은 데이터 적재를 시작한 시점 이후 부터만 조회가 가능하다는 것입니다. 그래서 데이터 추출에 대한 니즈가 생긴다면 최대한 빨리 만드는 것이 좋습니다. 

### 결론
데이터를 다루는 사람으로써 추출 요청을 많이 받다 보니, 사람들이 보고자 하는 정보들이 대게 비슷하다는 것을 느꼈습니다. 그 중에는 일반적인 쿼리로는 처리하기 까다로운 경우도 있었습니다. 물론 이미 서비스 및 데이터베이스 고도화가 진행된 회사라면 위와 같은 요청이 쉬울 확률이 높겠지만, 아직 다수의 스타트업은 그렇지 못한 경우가 많고 혹은 고도화가 완료되었더라도 데이터 처리는 복잡할 수 있습니다.  
그런 분들에게 오늘 소개해드린 방법이 조금이라도 도움이 되었으면 좋겠습니다.