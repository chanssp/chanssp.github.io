## 데이터 마트를 통한 데이터 추출 요청 처리 (더 알맞은 제목으로 수정 필요) 

---
### 개요
스타트업 데이터 팀에서 일하다보면 타 부서의 데이터 추출 요청을 정말 많이 받게됩니다.  
저희 회사의 경우 최근에 빅쿼리(BigQuery)를 이용한 데이터 웨어하우징 시스템을 도입했습니다. 빅쿼리는 구글에서 제공하는 데이터 저장 및 분석용 클라우드 서비스입니다. 일반적인 데이터베이스 시스템보다 훨씬 빠른 쿼리 처리 속도를 자랑하는 시스템이죠.
그래서 웬만한 데이터 추출 요청은 무리없이 처리할 수 있게 되었습니다.  
하지만, 정말 간혹 ~~끔찍한~~ 까다로운 요청이 들어올 때가 있습니다. 그런 까다로운 추출 요청을 데이터 엔지니어링과 결합해 풀어나가는 한 가지 방법을 소개하려고 합니다. 

### 예시
오늘의 요청 예시는 아래와 같습니다.
- (일별, 주별, 월별 조회 시) 모든 등록 상품의 누적판매가 0건인 호스트의 수
- (일별, 주별, 월별 조회 시) 모든 등록 상품의 누적판매가 0건인 호스트 중 판매전환된 호스트의 수  

사실 요청 내용만 놓고 보면 그리 복잡하지 않습니다. 저희 서비스는 판매자(host)가 입점해서 구매자(customer)와 만나는 형태의 플랫폼인데, 요청 내용을 정리하자면 입점은 했지만 아직 상품을 한번도 팔지 못한 호스트의 수를 일별로 또는 주별, 월별로 추출해 변화의 추이를 보고싶다는 것입니다.  
그런데 위 요청을 처리하는 과정은 일반적인 데이터 추출 요청보다 까다롭습니다. 이유인 즉슨, 요청한 정보는 '일별 누적'인데 데이터베이스에는 해당 내용에 대한 누적 통계를 저장하고 있지 않기 때문입니다.  

위 요청을 SQL로 처리하는 과정은 아래와 같습니다.  
```sql
with date_raw as (
  select dt Date_Ranges
  from unnest(generate_date_array('2021-10-01', '2021-10-30', interval 1 day)) dt
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
`date_join_host_raw`를 만들 때 크로스 조인을 한 테이블과 원본 호스트 테이블을 left join 하는데, 이 과정에서 `r.Date_Ranges >= h.created`라는 조건을 걸어줌으로써 각 호스트가 생성 이후 존재했던 모든 날짜에 매핑된 테이블이 완성됩니다. 
// purchase도 설명
// 결과 설명 및 테이블 예시 보여주기

위의 SQL 쿼리를 이용한 방식의 문제는 크로스 조인을 사용한다는 점입니다. 크로스 조인은 테이블을 엄청나게 뻥튀기해 처리하는 방식이기 때문에 컴퓨팅 파워도 많이 들고 한번 처리할 때 시간이 오래 걸리기 때문입니다. 

이런 문제를 해결하는 방법은 다양하겠지만, 오늘 소개해드릴 방법은 아주 약간의 데이터 엔지니어링을 섞은 해결책입니다. 

// 관리가 복잡해지기 때문에 모든 상황에 적용할 수는 없습니다. 그러나 해당 데이터를 주기적으로 사용할 예정이라면 위 방식처럼 데이터 마트(?!)를 만드는 것도 하나의 좋은 해결책이라고 생각됩니다. 마트에는 간단한 코드를 이용해 메타데이터를 상황에 맞게 추가할 수도 있기 때문에 한번 만들어 놓으면 여러 모로 확장성이 


결론

물론 이미 서비스 및 데이터베이스 고도화가 진행된 회사라면 경우에 따라 위와 같은 요청의 처리가 쉬울 수도 있습니다. 
하지만 많은 스타트업은 아직도 레거시 코드를 사용하기 때문에 