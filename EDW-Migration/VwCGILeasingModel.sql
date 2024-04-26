SELECT ccl."AcademicYear"
    , ccl."AcademicYearBeginDate"
    , ccl."AcademicYearEndDate"
    , ccl."Date"
    , ccl."DayName"
    , ccl."WeekBeginDate"
    , ccl."WeekEndDate"
    , ccl."CommunityName"
    , LAST_VALUE(ccl."PropertyCd") OVER (
        PARTITION BY ccl."CommunityName" ORDER BY ccl."Date" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "ExternalID"
    , ccl."CommunityGroupName"
    --    , ccl."TotalCommunityBeds"
    , crr."TotalCommunityBeds"
    , port."CGCellId"
    , port."CGGroupId"
    , port."OwnershipEntity" AS "OwnershipEntityName"
    , ccl."CGMgmtStatus"
    , port."EquityPartner" AS "Partner"
--    , ccl."Partner"
    , port."ManagementOperatingCompany" AS "MgmtOpCoName"
    , bud."Year Built"
    , bud."City"
    , bud."State"
    , bud."University"
    , bud."Dist. To Campus (mi.)"
    , bud."Fall Time Full Enrollment" AS "Fall Full Time Enrollment"
    , COALESCE(CAST(bud."Current Year New Supply" AS VARCHAR), '*') AS "Current Year New Supply"
    , crr.ManualPriorWeekTotalNetLeases
    , crr."ManualTotalSignedMonthlyRent"
    , crr."ManualCurrentAsking"
    , crr."ManualAverageLeased"
    , crr."ManualProjectedLeasedRate"
    , crr."ManualNetLeases"
    , pycrr."ManualPriorYearAverageLeased"
    , wrr."ManualWeeklyNetLeases"
    , wrr."Manual2PriorWeekyNetLeases"
    , py."pygrosspreleasedbedspacecnt"
    , py."pynetpreleasedbedspacecnt"
    , trn."DirectMarketBedsPreleased"
    , trn."DirectMarketTotalBeds"
    , trn."WholeMarketBedsPreleased"
    , trn."WholeMarketTotalBeds"
    , trn_py."PriorYearDirectMarketBedsPreleased"
    , trn_py."PriorYearDirectMarketTotalBeds"
    , trn_py."PriorYearWholeMarketBedsPreleased"
    , trn_py."PriorYearWholeMarketTotalBeds"
    , occ."OccupiedBedCnt"
    , fin."budgeted_occupancy"
    , AVG(his."Actual % for Prior Year") AS "ManualPriorYearLeasedPercent"
    , AVG(COALESCE(pycrr."executed_leases",0)) AS "PriorYearNetLeases"
    , AVG(ccl."GoalPreleasedBedSpaceCnt") AS "GoalPreleasedBedSpaceCnt"
    , AVG(ccl."GoalPreleasedBedSpaceCntAYTD") AS "GoalPreleasedBedSpaceCntAYTD"
    , AVG(ccl."GoalPreleasePct") AS "GoalPreleasePct"
    , SUM(ccl."GrossPreleaseExecutedCnt") AS "GrossPreleaseExecutedCnt"
    , SUM(ccl."GrossPreleasedBedSpaceCnt") AS "GrossPreleasedBedSpaceCnt"
    , SUM(ccl."NetPreleasedBedSpaceCnt") AS "NetPreleasedBedSpaceCnt"
    , SUM(ccl."NetNewPreleasedBedSpaceCnt") AS "NetNewPreleasedBedSpaceCnt"
    , SUM(ccl."NetRenewalStayPreleasedBedSpaceCnt") AS "NetRenewalStayPreleasedBedSpaceCnt"
    , SUM(ccl."NetRenewalTransferPreleasedBedSpaceCnt") AS "NetRenewalTransferPreleasedBedSpaceCnt"
    , SUM(ccl."SignedRentUSD") AS "SignedRentUSD"
    , SUM(ccl."SignedMonthlyRentUSD") AS "SignedMonthlyRentUSD"
    , SUM(ccl."OneTimeConcessionAmtUSD") AS "OneTimeConcessionAmtUSD"
    , SUM(ccl."RecurringConcessionAmtUSD") AS "RecurringConcessionAmtUSD"
    , SUM(ccl."MonthlyEffectiveRentUSD") AS "MonthlyEffectiveRentUSD"
    , AVG("CommittedConcessions") AS "CommittedConcessions"
    , AVG("Budgeted Rate") AS "Budgeted Rate"
    , AVG("Budgeted Concessions") AS "Budgeted Concessions"
    , AVG("Budgeted Occupancy") AS "Budgeted Occupancy"
    , AVG("PY Effective Rate") AS "PY Effective Rate"
    , AVG("Proforma Rate") AS "Proforma Rate"
    , AVG("Prior Year Rate") AS "Prior Year Rate"
    , DATE_TRUNC('week', CONVERT_TIMEZONE('US/Mountain', ccl."_BATCH_LAST_RUN_"))::DATE AS "lastupdatedon"
    , CASE 
        WHEN ccl."Date" = DATE_TRUNC('week', CONVERT_TIMEZONE('US/Mountain', ccl."_BATCH_LAST_RUN_"))::DATE - 1
            THEN 'Y'
        ELSE 'N'
        END AS "CurrentReportingWeekInd"
FROM "cumulative_community_pre_leasing_summary_student" ccl
INNER JOIN "cardinal_group_portfolio" port 
	ON ccl."PropertyCd" = port."PropertyCd"
LEFT JOIN "cgileasingmodelmanualentries" bud
    ON ccl."PropertyCd" = bud."ExternalID"
    	AND bud."AcademicYear" = '2024/2025'
LEFT JOIN (
    SELECT "as_of_date"
        , (
            CASE 
                WHEN "property_name" = 'Copper Social - Historical Access'
                    THEN 's411a'
                WHEN "property_name" = '12 North - Historical Access'
                    THEN 's397'
                ELSE "external_id"
                END
            ) AS "external_id"
        , SUM(new_lease_total_rent + renewal_stays_total_rent + renewal_transfers_total_rent) / NULLIF(SUM(executed_leases), 0) AS "ManualPriorYearAverageLeased"
  		, SUM(COALESCE("executed_leases",0)) AS "executed_leases"
  		, SUM(COALESCE("bed_count",0)) AS "bed_count"
    FROM "gcp_entrata_reporting_cumulative_rental_rate_summary_vw"
    GROUP BY 1
        , 2
    ) pycrr
    ON ccl."PropertyCd" = pycrr."external_id"
--    	AND ccl."Date" = pycrr."as_of_date"+364
        AND DATE_PART('year', ccl."Date") = DATE_PART('year', pycrr."as_of_date")+1
        AND DATE_PART('week', ccl."Date") = DATE_PART('week', pycrr."as_of_date")
        AND DATE_PART('dayofweek', ccl."Date") = DATE_PART('dayofweek', pycrr."as_of_date")
LEFT JOIN "historical_leasing_data_manual" his 
	ON ccl."PropertyCd" = his."Property ID"
    	AND ccl."Date" = his."Week Ending Date"
LEFT JOIN (
    SELECT crr."as_of_date"
        , crr."external_id"
        , crrpw."ManualPriorWeekTotalNetLeases"
        , SUM(crr."bed_count") AS "TotalCommunityBeds"
        , SUM(crr."total_signed_monthly_rent") AS "ManualTotalSignedMonthlyRent"
        , SUM(crr."monthly_asking_rent" * crr."bed_count") / NULLIF(SUM(crr."bed_count"), 0) AS "ManualCurrentAsking"
        , SUM(crr.new_lease_total_rent + crr.renewal_stays_total_rent + crr.renewal_transfers_total_rent) / NULLIF(SUM(crr.executed_leases), 0) AS "ManualAverageLeased"
        , SUM((crr.total_signed_monthly_rent + (crr.monthly_asking_rent * (crr.bed_count - crr.executed_leases)))) / NULLIF(SUM(crr.bed_count), 0) AS "ManualProjectedLeasedRate"
        , SUM(crr."executed_leases") AS "ManualNetLeases"
    FROM "gcp_entrata_reporting_cumulative_rental_rate_summary_vw" crr
    LEFT JOIN (
        SELECT "as_of_date"
            , "external_id"
            , SUM("executed_leases") AS "ManualPriorWeekTotalNetLeases"
        FROM "gcp_entrata_reporting_cumulative_rental_rate_summary_vw"
        GROUP BY 1
            , 2
        ) crrpw
        ON crr."external_id" = crrpw."external_id"
            AND crr."as_of_date" = crrpw."as_of_date" + 7
    GROUP BY 1
        , 2
        , 3
    ) crr
    ON ccl."PropertyCd" = crr."external_id"
        AND ccl."Date" = crr."as_of_date"
LEFT JOIN (
    SELECT wrr."week_end_date"
        , wrr."external_id"
        , wrrpw."Manual2PriorWeekyNetLeases"
        , SUM(wrr."executed_leases") AS "ManualWeeklyNetLeases"
    FROM "gcp_entrata_reporting_weekly_rental_rate_summary_vw" wrr
    LEFT JOIN (
        SELECT "week_end_date"
            , "external_id"
            , SUM("executed_leases") AS "Manual2PriorWeekyNetLeases"
        FROM "gcp_entrata_reporting_weekly_rental_rate_summary_vw"
        GROUP BY 1
            , 2
        ) wrrpw
        ON wrr."external_id" = wrrpw."external_id"
            AND wrr."week_end_date" = wrrpw."week_end_date" + 7
    GROUP BY 1
        , 2
        , 3
    ) wrr
    ON ccl."PropertyCd" = wrr."external_id"
        AND ccl."Date" = wrr."week_end_date"
LEFT JOIN (
    SELECT "AcademicYear"
        , "Date"
        , "CommunityName"
        , SUM("GrossPreleasedBedSpaceCnt") AS pyGrossPreleasedBedSpaceCnt
        , SUM("NetPreleasedBedSpaceCnt") AS pyNetPreleasedBedSpaceCnt
        , SUM("NetNewPreleasedBedSpaceCnt") AS pyNetNewPreleasedBedSpaceCnt
        , SUM("NetRenewalStayPreleasedBedSpaceCnt") AS pyNetRenewalStayPreleasedBedSpaceCnt
        , SUM("NetRenewalTransferPreleasedBedSpaceCnt") AS pyNetRenewalTransferPreleasedBedSpaceCnt
        , SUM("SignedRentUSD") AS pysignedrent
        , SUM("SignedMonthlyRentUSD") AS pySignedMonthlyRentUSD
        , SUM("OneTimeConcessionAmtUSD") AS pyonetimeconcession
        , SUM("RecurringConcessionAmtUSD") AS pyrecurringconcession
        , SUM("MonthlyEffectiveRentUSD") AS pymonthlyeffectiverent
    FROM "cumulative_community_pre_leasing_summary_student" py
    GROUP BY 1
        , 2
        , 3
    ) py
    ON ccl."CommunityName" = py."CommunityName"
        AND ccl."Date" = py."Date" + 364
        AND py."AcademicYear" = '2023/2024'
LEFT JOIN (
    SELECT "reporting_week_end_date"
        , "external_id_subject"
        , SUM(CASE 
                WHEN "impact" = 'High'
                    THEN "plr_beds_preleased"
                ELSE 0
                END) AS "DirectMarketBedsPreleased"
        , SUM(CASE 
                WHEN "impact" = 'High'
                    THEN "beds"
                ELSE 0
                END) AS "DirectMarketTotalBeds"
        , SUM(CASE 
                WHEN "impact" != 'Subject Community'
                    THEN "plr_beds_preleased"
                ELSE 0
                END) AS "WholeMarketBedsPreleased"
        , SUM(CASE 
                WHEN "impact" != 'Subject Community'
                    THEN "beds"
                ELSE 0
                END) AS "WholeMarketTotalBeds"
    FROM "gcp_terrain_psql_reporting_trn_plr_occupancy_vw"
    GROUP BY 1
        , 2
    ) trn
    ON ccl."WeekEndDate" = trn."reporting_week_end_date"
        AND ccl."PropertyCd" = trn."external_id_subject"
LEFT JOIN (
    SELECT "reporting_week_end_date"
        , "external_id_subject"
        , SUM(CASE 
                WHEN "impact" = 'High'
                    THEN "plr_beds_preleased"
                ELSE 0
                END) AS "PriorYearDirectMarketBedsPreleased"
        , SUM(CASE 
                WHEN "impact" = 'High'
                    THEN "beds"
                ELSE 0
                END) AS "PriorYearDirectMarketTotalBeds"
        , SUM(CASE 
                WHEN "impact" != 'Subject Community'
                    THEN "plr_beds_preleased"
                ELSE 0
                END) AS "PriorYearWholeMarketBedsPreleased"
        , SUM(CASE 
                WHEN "impact" != 'Subject Community'
                    THEN "beds"
                ELSE 0
                END) AS "PriorYearWholeMarketTotalBeds"
    FROM "gcp_terrain_psql_reporting_trn_plr_occupancy_vw"
    GROUP BY 1
        , 2
    ) trn_py
    ON ccl."WeekEndDate" = DATEADD(week, 52, trn_py."reporting_week_end_date")
        AND ccl."PropertyCd" = trn_py."external_id_subject"
LEFT JOIN (
    SELECT "Date"
        , "ExternalID" AS "PropertyCd"
        , SUM("OccupiedBedCnt") AS "OccupiedBedCnt"
    FROM "community_floorplan_occupancy"
    GROUP BY 1
        , 2
    ) occ
    ON ccl."Date" = occ."Date"
        AND ccl."PropertyCd" = occ."PropertyCd"
LEFT JOIN (
    SELECT "post_month_date"
        , "external_id"
        , (
            (
                sum(CASE 
                        WHEN "gl_account_name" = 'Rent - Potential'
                            THEN "budget_amount"
                        ELSE 0
                        END) + sum(CASE 
                        WHEN "gl_account_name" = 'Rent - Vacancy'
                            THEN "budget_amount"
                        ELSE 0
                        END)
                ) / NULLIF(sum(CASE 
                        WHEN "gl_account_name" = 'Rent - Potential'
                            THEN "budget_amount"
                        ELSE 0
                        END), 0)
            ) AS budgeted_occupancy
    FROM "gcp_entrata_psql_reporting_gl_account_trial_balance_with_budget_nightly_vw"
    GROUP BY 1
        , 2
    ) fin
    ON ccl."PropertyCd" = fin."external_id"
        AND DATE_PART(month, ccl."Date") = DATE_PART(month, fin."post_month_date")
        AND DATE_PART(year, ccl."Date") = DATE_PART(year, fin."post_month_date")
WHERE port."OwnershipEntity" LIKE '%Cardinal Group Investments%'
    AND UPPER(ccl."CGMgmtStatus") = 'CURRENT'
    AND ccl."CommunityName" NOT LIKE '%Historical%'
    AND port."PropertyType" = 'Student'
    AND ccl."AcademicYear" IN ('2023/2024', '2024/2025')
    AND ccl."DayName" = 'Sunday'
GROUP BY ccl."AcademicYear"
    , ccl."AcademicYearBeginDate"
    , ccl."AcademicYearEndDate"
    , ccl."Date"
    , ccl."DayName"
    , ccl."WeekBeginDate"
    , ccl."WeekEndDate"
    , ccl."CommunityName"
    , ccl."PropertyCd"
    , ccl."CommunityGroupName"
    , crr."TotalCommunityBeds"
    , ccl."PropertyType"
    , port."CGCellId"
    , port."CGGroupId"
    , port."OwnershipEntity"
    , ccl."CGMgmtStatus"
    , port."EquityPartner"
--    , ccl."Partner"
    , port."ManagementOperatingCompany"
    , ccl."_BATCH_LAST_RUN_"
    , bud."Year Built"
    , bud."City"
    , bud."State"
    , bud."University"
    , bud."Dist. To Campus (mi.)"
    , bud."Fall Time Full Enrollment"
    , bud."Current Year New Supply"
    , crr.ManualPriorWeekTotalNetLeases
    , crr."ManualTotalSignedMonthlyRent"
    , crr."ManualCurrentAsking"
    , crr."ManualAverageLeased"
    , crr."ManualProjectedLeasedRate"
    , crr."ManualNetLeases"
    , pycrr."ManualPriorYearAverageLeased"
    , wrr."ManualWeeklyNetLeases"
    , wrr."Manual2PriorWeekyNetLeases"
    , py."pygrosspreleasedbedspacecnt"
    , py."pynetpreleasedbedspacecnt"
    , trn."DirectMarketBedsPreleased"
    , trn."DirectMarketTotalBeds"
    , trn."WholeMarketBedsPreleased"
    , trn."WholeMarketTotalBeds"
    , trn_py."PriorYearDirectMarketBedsPreleased"
    , trn_py."PriorYearDirectMarketTotalBeds"
    , trn_py."PriorYearWholeMarketBedsPreleased"
    , trn_py."PriorYearWholeMarketTotalBeds"
    , occ."OccupiedBedCnt"
    , fin."budgeted_occupancy"
--    , pycrr."PriorYearLeasedPercent"
--    , his."Actual % for Prior Year"
ORDER BY ccl."Date" DESC
    , ccl."CommunityName" DESC
