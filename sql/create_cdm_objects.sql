CREATE SCHEMA cdm;

CREATE OR REPLACE VIEW cdm.events_by_hour AS

select to_char(e.event_timestamp,'HH24') event_hour,
       date(e.event_timestamp) event_date,
       case when substring(e.page_url_path ,2) like 'product_%' then 'product' else substring(e.page_url_path ,2) end as event_type,
       count(1) event_count
  from stg.events e 
 where e.user_domain_id = '32e53fe3-71b6-4b66-ab8c-9a79d544b912'
   and event_timestamp between '2022-09-30 00:00:00' and '2022-10-01 23:59:59'
 group by to_char(e.event_timestamp,'HH24'),
          date(e.event_timestamp),
          case when substring(e.page_url_path ,2) like 'product_%' then 'product' else substring(e.page_url_path ,2) end;


CREATE OR REPLACE VIEW cdm.top_pages_prior_payment
AS SELECT t.prev_url_path,
    t.prev_page_count
   FROM ( SELECT c.prev_url_path,
            count(1) AS prev_page_count
           FROM ( SELECT e.page_url_path,
                    lag(e.page_url_path) OVER (ORDER BY e.user_custom_id, e.event_timestamp) AS prev_url_path
                   FROM stg.events e) c
          WHERE c.page_url_path::text = '/payment'::text
          GROUP BY c.prev_url_path) t
  ORDER BY t.prev_page_count DESC
 LIMIT 10;