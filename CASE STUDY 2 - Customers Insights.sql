--IMPORTANT: The followings querys were execute in PostgreSQL. I've imported the tables public.funneltable and public.bookingtable from "SQL data.xlsx" file.

--public.bookingtable ->   337 rows
--public.funneltable -> 10.003 rows

--1. What is the total revenue generated per each country AND device the 2nd of November?

SELECT fl.country
      ,fl.device
	  ,SUM(bk.revenue) AS revenue
FROM public.funneltable AS fl INNER JOIN public.bookingtable AS bk ON fl.transactionid = bk.transactionid
WHERE CAST(fl.timestamp AS date) = '20191102'
GROUP BY fl.country,fl.device
ORDER BY revenue DESC;


--2. What product from Paid Search channel has the highest average revenue per transaction?

SELECT bk.product
      ,CAST(SUM(bk.revenue)/COUNT(1) AS numeric(6,2)) AS revenue_avg
FROM public.funneltable AS fl INNER JOIN public.bookingtable AS bk ON fl.transactionid = bk.transactionid
WHERE fl.channel = 'Paid Search'
GROUP BY bk.product
ORDER BY revenue_avg DESC
LIMIT 1


--3. How many users visited us between 21:00 and 22:00 on the 2nd of November?

SELECT COUNT(DISTINCT userid) AS users_count
FROM public.funneltable AS fl
WHERE fl.timestamp BETWEEN '2019-11-02 21:00:00' AND '2019-11-02 22:00:00'


--4. What country has the most pageviews per session on average?

--calculates count of pageviews per session (sessionid-userid)
WITH pageviews_session AS ( 
	SELECT fl.country
	      ,fl.sessionid
	      ,fl.userid
	      ,COUNT(1) AS pageviews
	FROM public.funneltable AS fl
	GROUP BY fl.country,fl.sessionid,fl.userid
)

SELECT country
      ,CAST(SUM(pageviews)/COUNT(1) AS numeric(5,2)) AS pageviews_avg
FROM pageviews_session
GROUP BY country
ORDER BY pageviews_avg DESC
LIMIT 1


--5. How many users made more than one transaction?

--calculates count of transactions per userid that made more than one transaction
WITH users_transactions AS (
	SELECT fl.userid
	      ,count(bk.transactionid) as transactions_count
	FROM public.funneltable AS fl INNER JOIN public.bookingtable AS bk ON fl.transactionid = bk.transactionid
	GROUP BY fl.userid
	HAVING COUNT(bk.transactionid)>1
)

SELECT COUNT(1) AS users_count
FROM users_transactions


--6. How many sessions have seen the “home” page AND the “payment” page?

--calculates sessions (sessionid-userid concatenated) that have seen "home" page
WITH home AS (
		SELECT page
	          ,sessionid||userid AS session
		FROM public.funneltable 
		WHERE page = 'home'
),--calculates sessions (sessionid-userid concatenated) that have seen "payment" page 
payment AS (
		SELECT page
	          ,sessionid||userid AS session
		FROM public.funneltable 
		WHERE page = 'payment'
)
--counts distinct sessions (sessionid-userid concatenated) that have seen "home" and "payment" pages
SELECT count(DISTINCT h.session) AS sessions_count
FROM home AS h INNER JOIN payment AS p ON h.session=p.session


--7. We define “continuance rate” as the percentage of sessions that have progressed from payment to confirmation, i.e. (sessions that have seen confirmation) / (sessions that have seen payment). Compute it by country AND device.


--counts distinct sessions (sessionid-userid concatenated) that have seen confirmation per country and device
WITH confirmation AS (
	SELECT country
	      ,device
	      ,COUNT(DISTINCT sessionid||userid) AS session_count
	FROM public.funneltable
	WHERE page = 'confirmation'
	GROUP BY country, device
)

SELECT fl.country
      ,fl.device
	  ,COALESCE(CAST(CAST(con.session_count AS NUMERIC(6,2))/CAST(COUNT(DISTINCT fl.sessionid||fl.userid)AS NUMERIC(6,2)) AS NUMERIC(6,2)),0) AS continuance_rate
FROM public.funneltable AS fl LEFT JOIN confirmation AS con ON fl.country = con.country AND fl.device = con.device
WHERE fl.page = 'payment'
GROUP BY fl.country, fl.device,con.session_count
ORDER BY continuance_rate DESC


--8. We define “landing page” as the first page in a session. What is the most abundant landing page?

--calculates a row number as page position per session (sessionid-userid)
WITH funnel_landing AS (
	SELECT row_number() OVER(PARTITION BY sessionid,userid ORDER BY timestamp ASC) AS page_position
	      ,*
	FROM public.funneltable
)

SELECT page
      ,count(1) as page_count
FROM funnel_landing
WHERE page_position = 1 --only "landing pages"
GROUP BY page
ORDER BY page_count DESC
LIMIT 1

--9. We define conversion rate as (total transactions) / (total sessions). What is the conversion rate per landing page AND device?

--calculates transactionid value as a new field in the same row of landing page (row_number=1)
WITH funnel_landing_tran AS (
	SELECT ROW_NUMBER() OVER(PARTITION BY fl.sessionid,fl.userid ORDER BY fl.timestamp ASC) AS page_position
	      ,*
	FROM public.funneltable as fl
	LEFT JOIN LATERAL 
						(  SELECT transactionid AS confirmation_tran
						   FROM public.funneltable
						   WHERE sessionid = fl.sessionid and userid=fl.userid
						   AND page = 'confirmation'
						)  transaction 
     ON true

), --counts transactionid per landing page and device
funnel_page_device_tran AS(
	
	SELECT page AS landing_page
	      ,device
	      ,count(confirmation_tran) AS tran_count
	FROM funnel_landing_tran
	WHERE page_position = 1
	GROUP BY page,device

),--counts sessions (sessionid-userid concatenated) per landing page and device
funnel_page_device_session AS(

	SELECT page AS landing_page
	      ,device
	      ,COUNT(DISTINCT sessionid||userid) AS session_count
	FROM funnel_landing_tran 
	WHERE page_position = 1
	GROUP BY page,device
)


SELECT fpds.landing_page
      ,fpds.device
	  ,COALESCE(CAST(CAST(fpdt.tran_count AS NUMERIC(6,2))/CAST(fpds.session_count AS NUMERIC(6,2)) AS NUMERIC(6,2)),0) AS conversion_rate
FROM funnel_page_device_session as fpds LEFT JOIN funnel_page_device_tran AS fpdt ON fpds.landing_page = fpdt.landing_page and fpds.device = fpdt.device
ORDER BY conversion_rate DESC


--10. We define “Exit rate” as the percentage of times a page was the last one in a session, out of all the times the page was viewed. What is the exit rate of “results”?

--calculates a row number as page position per session (sessionid-userid)
WITH funnel_page_position AS (
	SELECT row_number() OVER(PARTITION BY sessionid,userid ORDER BY timestamp ASC) AS page_number
	      ,*
	FROM public.funneltable
),--calculates if results was the max page position per session (sessionid-userid)
funnel_results AS (
	
	SELECT CASE WHEN fpp.page_number = max_page_position.max_position THEN 1 ELSE 0 END AS exit
	      ,max_page_position.max_position
		  ,fpp.*
	FROM funnel_page_position AS fpp
	LEFT JOIN LATERAL 
						(  SELECT MAX(page_number) AS max_position
						   FROM funnel_page_position
						   WHERE sessionid = fpp.sessionid AND userid=fpp.userid
						)  max_page_position
     ON true
	WHERE fpp.page = 'results'
)

SELECT COALESCE(CAST(CAST(SUM(exit) AS NUMERIC(6,2))/CAST(COUNT(1) AS NUMERIC(6,2)) AS NUMERIC(6,2)),0) AS results_exit_rate
FROM funnel_results