WITH snp AS (
    SELECT
        external_id AS "id",
        cg_group,
        cell,
  		client,
        property_name
    FROM 
        gcp_entrata_psql_reporting_plr_clr_timeseries_vw
    WHERE 
        snapshot_date = CURRENT_DATE - 1
        AND UPPER("status") = 'CURRENT'
        AND student_or_conventional = 'Student'
        AND domo_y_n = 'Y'
        AND property_name NOT LIKE '%Historical%'
),
port AS (
    SELECT 
        PropertyCd AS "id",
        KeystoneReportingInd,
        PortfolioSalesManager AS PortfolioSlsMgrName
    FROM 
        cardinal_group_portfolio
),
trn AS (
    SELECT
        "external_id_subject" AS "id",
        SUM(CASE 
            	WHEN "impact" = 'Subject Community' THEN 0
            	ELSE "plr_beds_preleased"
            END) 
  		/
        NULLIF(SUM(CASE 
            	WHEN "impact" = 'Subject Community' THEN 0
            	ELSE "beds"
            END),0) AS "MarketAveragePercent",
  		(CASE WHEN 
            (CASE 
                WHEN SUM(plr_beds_available) = 0 THEN 0
                ELSE 
                (SUM(CASE WHEN impact = 'Subject Community' THEN ((beds * goal_percent_year_end) - plr_beds_preleased) ELSE 0 END)  
                / 
                NULLIF((SUM(occupancy_pct * beds) - SUM(CASE WHEN occupancy_pct = 0 THEN 0 ELSE plr_beds_preleased END)), 0))
            END) < 0 THEN 0 ELSE
            (CASE 
                WHEN SUM(plr_beds_available) = 0 THEN 0
                ELSE 
                (SUM(CASE WHEN impact = 'Subject Community' THEN ((beds * goal_percent_year_end) - plr_beds_preleased) ELSE 0 END)  
                / 
                NULLIF((SUM(occupancy_pct * beds) - SUM(CASE WHEN occupancy_pct = 0 THEN 0 ELSE plr_beds_preleased END)), 0))
            END)
        END)
        / NULLIF(SUM(CASE WHEN impact = 'Subject Community' THEN beds ELSE 0 END) / NULLIF(SUM(CASE WHEN occupancy_pct = 0 THEN 0 ELSE beds END), 0), 0) AS absorption_multiple
    FROM 
        gcp_terrain_psql_reporting_trn_plr_occupancy_vw
    WHERE 
        current_reporting_week_ind = 'Y'
    GROUP BY
        "external_id_subject"
),
trn_py AS (
    SELECT
        "external_id_subject" AS "id", 
        SUM(CASE 
            	WHEN "impact" = 'Subject Community' THEN 0
            	ELSE "plr_beds_preleased"
            END) 
  		/
        NULLIF(SUM(CASE 
            	WHEN "impact" = 'Subject Community' THEN 0
            	ELSE "beds"
            END),0) AS "PriorYearMarketAveragePercent"
    FROM 
        gcp_terrain_psql_reporting_trn_plr_occupancy_vw
    WHERE DATE_PART('week', "reporting_week_end_date") = DATE_PART('week', CURRENT_DATE-1)
  		AND DATE_PART('year', "reporting_week_end_date") = DATE_PART('year', CURRENT_DATE-1)-1
        AND status_subject = 'Current'
  		AND "student_or_conventional" = 'Student'
    GROUP BY
        "external_id_subject"
),
lyfet30 AS (
    SELECT external_id AS "id",
        SUM(CASE WHEN lease_approved_flag = 1 THEN 1 ELSE 0 END) AS leases_approved_t30,
        COUNT(rowcount) AS total_leads_t30    
  	FROM 
        gcp_entrata_psql_reporting_lead_lifecycle_vw
    WHERE 
        student_or_conventional = 'Student'
        AND status = 'CURRENT'
        AND guest_card_created_timestamp >= (CURRENT_DATE - 30)
    GROUP BY 
        external_id
),
openpositions AS (
    SELECT 
        external_id AS "id",
        COUNT(DISTINCT requisition_number) AS open_positions
    FROM 
        gcp_ultipro_reporting_ultipro_recruiting_vw
    WHERE 
        opportunity_status = 'Published'
        AND job_family = 'Leasing'
    GROUP BY 
        external_id
),
ora AS (
    SELECT 
        external_id AS "id",
        NULLIF(ora_score,0)
    FROM 
        j_turner_scores_enhanced
    WHERE 
        is_recent_month = 1
),
subjectrate AS (
	SELECT 
        external_id_subject AS "id",
        (sum((CASE WHEN (NOT ("impact" = 'Subject Community')) THEN 0 ELSE ("effective_rent" * "unit_cnt") END)) / NULLIF(sum((CASE WHEN (NOT ("impact" = 'Subject Community')) THEN 0 ELSE "unit_cnt" END)), 0)) AS "subjectrate"
    FROM 
        "gcp_terrain_psql_reporting_trn_unit_mix_vw"
    WHERE 
        student_or_conventional_subject = 'Student'
        AND current_reporting_week_ind = 'Y'
    GROUP BY 1
  ),
  marketrate AS (
	SELECT 
        external_id_subject AS "id",
        (sum((CASE WHEN (("impact" = 'Subject Community')) THEN 0 ELSE ("effective_rent" * "unit_cnt") END)) / NULLIF(sum((CASE WHEN (("impact" = 'Subject Community')) THEN 0 ELSE "unit_cnt" END)), 0)) AS "marketrate"
    FROM 
        "gcp_terrain_psql_reporting_trn_unit_mix_vw"
    WHERE 
        student_or_conventional_subject = 'Student'
        AND current_reporting_week_ind = 'Y'
    GROUP BY 1
 ),
conv AS (
  SELECT "external_id" AS "id",
    NULLIF(
    AVG(
        CAST(DATEDIFF(day, "guest_card_created_timestamp", COALESCE("lease_approved_timestamp", CURRENT_DATE)) AS FLOAT)
    ),
    0
)
 AS time_to_convert
FROM "gcp_entrata_psql_reporting_lead_lifecycle_vw"
WHERE "guest_card_created_timestamp" >= CURRENT_DATE-31
GROUP BY "external_id"
)
  
  SELECT snp."id"
    , snp.property_name
    , snp.cg_group
    , snp.cell
    , snp.client AS "OwnershipEntityName"
    , port.KeystoneReportingInd
    , port.PortfolioSlsMgrName
	, ora.ora_score::NUMERIC AS "ora"
    , op."open_positions"
    , ((ll."leases_approved_t30"::decimal)/(NULLIF(ll."total_leads_t30"::decimal,0::decimal))) AS t30_conversion_ratio 
    , trn."absorption_multiple"
    , trn."MarketAveragePercent" - trn_py."PriorYearMarketAveragePercent" AS "yoy_market_average"
    , (sr."subjectrate" - mr."marketrate") / NULLIF(mr."marketrate",0) AS "pct_rate_variance_to_market"
    , conv."time_to_convert"::FLOAT
    , (CASE 
    	WHEN t30_conversion_ratio >= 0.25 THEN 'Lowest' 
        WHEN t30_conversion_ratio BETWEEN 0.2 AND .249 THEN 'Low' 
        WHEN t30_conversion_ratio BETWEEN 0.15 AND .199 THEN 'Moderate' 
        WHEN t30_conversion_ratio BETWEEN 0.1 AND .149 THEN 'High' 
        WHEN t30_conversion_ratio BETWEEN 0 AND .099 THEN 'Highest' 
        ELSE 'No Data'
      END) AS t30_conversion_ratio_score_label
    , (CASE 
    	WHEN open_positions IS NULL THEN 'No Risk' 
        WHEN open_positions = 1 THEN 'Moderate' 
        WHEN open_positions BETWEEN 2 AND 3 THEN 'High' 
        WHEN open_positions > 3 THEN 'Highest' 
      END) AS open_positions_score_label
    , (CASE 
    	WHEN "absorption_multiple" <= 1 THEN 'Lowest' 
        WHEN "absorption_multiple" BETWEEN 1.01 AND 1.2 THEN 'Low' 
        WHEN "absorption_multiple" BETWEEN 1.201 AND 1.4 THEN 'Moderate' 
        WHEN "absorption_multiple" BETWEEN 1.401 AND 1.6 THEN 'High' 
        WHEN "absorption_multiple" >= 1.601 THEN 'Highest' 
        ELSE 'No Data'
      END) AS absorption_multiple_score_label
    , (CASE 
    	WHEN "yoy_market_average" <= 0 THEN 'Lowest' 
        WHEN "yoy_market_average" BETWEEN 0.01 AND 0.04 THEN 'Low' 
        WHEN "yoy_market_average" BETWEEN 0.0401 AND 0.06 THEN 'Moderate' 
        WHEN "yoy_market_average" BETWEEN 0.0601 AND 0.08 THEN 'High' 
        WHEN "yoy_market_average" > 0.08 THEN 'Highest' 
        ELSE 'No Data'
      END) AS yoy_market_average_score_label
    , (CASE 
    	WHEN "pct_rate_variance_to_market" <= 0 THEN 'Lowest' 
        WHEN "pct_rate_variance_to_market" BETWEEN 0.01 AND 0.04 THEN 'Low' 
        WHEN "pct_rate_variance_to_market" BETWEEN 0.0401 AND 0.06 THEN 'Moderate' 
        WHEN "pct_rate_variance_to_market" BETWEEN 0.0601 AND 0.08 THEN 'High' 
        WHEN "pct_rate_variance_to_market" > 0.08 THEN 'Highest' 
        ELSE 'No Data'
      END) AS pct_rate_variance_to_market_score_label
    , (CASE 
    	WHEN "time_to_convert" <= 15 THEN 'Lowest' 
        WHEN "time_to_convert" BETWEEN 16 AND 20 THEN 'Low' 
        WHEN "time_to_convert" BETWEEN 21 AND 25 THEN 'Moderate' 
        WHEN "time_to_convert" BETWEEN 26 AND 30 THEN 'High' 
        WHEN "time_to_convert" > 30 THEN 'Highest' 
        ELSE 'No Data'
      END) AS time_to_convert_lead_score_label
    , (CASE 
    	WHEN "ora" >= 65 THEN 'Lowest' 
        WHEN "ora" BETWEEN 60 AND 64 THEN 'Low' 
        WHEN "ora" BETWEEN 55 AND 59 THEN 'Moderate' 
        WHEN "ora" BETWEEN 50 AND 54 THEN 'High' 
        WHEN "ora" < 50 THEN 'Highest' 
        ELSE 'No Data'
      END) AS ora_score_label
    , (CASE 
    	WHEN t30_conversion_ratio >= 0.25 THEN 1 
        WHEN t30_conversion_ratio BETWEEN 0.2 AND .249 THEN 1.5 
        WHEN t30_conversion_ratio BETWEEN 0.15 AND .199 THEN 2 
        WHEN t30_conversion_ratio BETWEEN 0.1 AND .149 THEN 2.5 
        WHEN t30_conversion_ratio BETWEEN 0 AND .099 THEN 3 
        ELSE 0
      END) * 0.15 AS t30_conversion_ratio_score
    , (CASE 
    	WHEN open_positions IS NULL THEN 0 
        WHEN open_positions = 1 THEN 2 
        WHEN open_positions BETWEEN 2 AND 3 THEN 2.5 
        WHEN open_positions > 3 THEN 3 
      END) * 0.025 AS open_positions_score
    , (CASE 
    	WHEN "absorption_multiple" <= 1 THEN 1 
        WHEN "absorption_multiple" BETWEEN 1.01 AND 1.2 THEN 1.5 
        WHEN "absorption_multiple" BETWEEN 1.201 AND 1.4 THEN 2 
        WHEN "absorption_multiple" BETWEEN 1.401 AND 1.6 THEN 2.5 
        WHEN "absorption_multiple" >= 1.601 THEN 3 
        ELSE 0
      END) * 0.2 AS absorption_multiple_score
    , (CASE 
    	WHEN "yoy_market_average" <= 0 THEN 1 
        WHEN "yoy_market_average" BETWEEN 0.01 AND 0.04 THEN 1.5 
        WHEN "yoy_market_average" BETWEEN 0.0401 AND 0.06 THEN 2 
        WHEN "yoy_market_average" BETWEEN 0.0601 AND 0.08 THEN 2.5 
        WHEN "yoy_market_average" > 0.08 THEN 3 
        ELSE 0
      END) * 0.2 AS yoy_market_average_score
    , (CASE 
    	WHEN "pct_rate_variance_to_market" <= 0 THEN 1 
        WHEN "pct_rate_variance_to_market" BETWEEN 0.01 AND 0.04 THEN 1.5 
        WHEN "pct_rate_variance_to_market" BETWEEN 0.0401 AND 0.06 THEN 2 
        WHEN "pct_rate_variance_to_market" BETWEEN 0.0601 AND 0.08 THEN 2.5 
        WHEN "pct_rate_variance_to_market" > 0.08 THEN 3 
        ELSE 0
      END) * 0.1 AS pct_rate_variance_to_market_score
    , (CASE 
    	WHEN "time_to_convert" <= 15 THEN 1 
        WHEN "time_to_convert" BETWEEN 16 AND 20 THEN 1.5 
        WHEN "time_to_convert" BETWEEN 21 AND 25 THEN 2 
        WHEN "time_to_convert" BETWEEN 26 AND 30 THEN 2.5 
        WHEN "time_to_convert" > 30 THEN 3 
        ELSE 0
      END) * 0.025 AS time_to_convert_lead_score
    , (CASE 
    	WHEN "ora" >= 65 THEN 1 
        WHEN "ora" BETWEEN 60 AND 64 THEN 1.5 
        WHEN "ora" BETWEEN 55 AND 59 THEN 2 
        WHEN "ora" BETWEEN 50 AND 54 THEN 2.5 
        WHEN "ora" < 50 THEN 3 
        ELSE 0
      END) * 0.3 AS ora_score
    FROM 
        snp
    LEFT JOIN 
        trn ON snp."id" = trn."id"
    LEFT JOIN 
        trn_py ON snp."id" = trn_py."id"
    LEFT JOIN 
        lyfet30 ll ON snp."id" = ll."id"
    LEFT JOIN 
        openpositions op ON snp."id" = op."id"
    LEFT JOIN 
        port ON snp."id" = port."id"
    LEFT JOIN 
        ora ON snp."id" = ora."id"
    LEFT JOIN 
    	subjectrate sr ON snp."id" = sr."id"
    LEFT JOIN 
    	marketrate mr ON snp."id" = mr."id"
    LEFT JOIN 
    	conv ON snp."id" = conv."id"