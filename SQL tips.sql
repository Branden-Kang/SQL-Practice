-- Thanks to https://medium.com/better-programming/4-sql-tips-for-data-scientist-and-data-engineers-56c41487752f

SELECT patient_county
	,avg(avg_visits_per_patient)*1.0 avg_visits
	,avg(avg_cost_per_patient)*1.0 avg_cost
    ,'Avg of Averages' table_type
FROM agg_patient_counties 
GROUP BY patient_county,table_type

--Don’t Use Avg() on an Average

SELECT 
    patient_county
    ,count(*)*1.0/count(distinct c.patient_id) avg_total_visits
    ,sum(cast(claim_cost AS decimal))/count(distinct c.patient_id) avg_total_costs
    ,'Correct Way' as table_type
FROM patients p
JOIN claims c ON p.patient_id = c.patient_id
GROUP by patient_county,table_type


SELECT total_claims_over_500 * 100.0 / COUNT(*)
FROM claims
JOIN (
	SELECT COUNT(*) total_claims_over_500
	FROM claims
	WHERE CAST(claim_cost AS INT) > 500
	) t1 ON 1 = 1
GROUP BY total_claims_over_500

-- You Can Use A Case Statement Inside Sum

SELECT SUM(CASE 
		WHEN CAST(claim_cost AS INT) > 500
			THEN 1
		ELSE 0
		END) * 100.0 / COUNT(*) perc_claims_over_500
FROM claims

--Understanding Arrays and How to Manipulate Them

SELECT username
	,key
	,value
FROM user_info u
	,json_each_text(user_data) i
  
-- Lead and Lag to Avoid Self Joins

WITH claim_cte
AS (
	SELECT patient_id
		,claim_date claim_date
		,claim_cost
		,lag(claim_date) OVER (
			PARTITION BY patient_id ORDER BY claim_date
			) previous_claim_date
		,lag(claim_cost) OVER (
			PARTITION BY patient_id ORDER BY claim_date
			) previous_claim_cost
	FROM claims
	)
SELECT claim_date - previous_claim_date days_between_dates
	,patient_id
	,claim_date
	,claim_cost
	,previous_claim_date
	,previous_claim_cost
FROM claim_cte
