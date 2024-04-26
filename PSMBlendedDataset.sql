WITH openpositions AS (
    SELECT 
        ccl."Date" AS op_date,
        ccl."PropertyCd" AS op_id,
        COUNT(DISTINCT up."requisition_number") AS "OpenLeasingPositions"
    FROM "cumulative_community_pre_leasing_summary_student" ccl
    LEFT JOIN "gcp_ultipro_reporting_ultipro_recruiting_vw" up
        ON ccl."PropertyCd" = up."external_id"
        AND (
            (up."opportunity_status" = 'Published' AND ccl."Date" >= up."first_published_date")
            OR (up."opportunity_status" <> 'Published' AND ccl."Date" BETWEEN up."first_published_date" AND up."closed_date")
        )
    WHERE UPPER(ccl."CGMgmtStatus") = 'CURRENT'
        AND ccl."PropertyType" = 'Student'
        AND ccl."KeystoneReportingInd" = 'Y'
    GROUP BY 1, 2
),
DailyTotals AS (
    SELECT
        "Date",
        "WeekBeginDate",
        "PropertyCd",
        SUM(COALESCE("NetLeasedBedSpaceCnt",0)) AS daily_netleasedbedspacecnt
    FROM "community_pre_leasing_summary_student"
    GROUP BY 1, 2, 3
),
RunningTotals AS (
    SELECT
        "Date",
        "WeekBeginDate",
        "PropertyCd",
        daily_netleasedbedspacecnt,
        SUM(daily_netleasedbedspacecnt) OVER (PARTITION BY "WeekBeginDate", "PropertyCd" ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "WeekToDateNetLeasedBedSpaceCnt"
    FROM DailyTotals
),
trn_sub AS (
    SELECT 
        trn."external_id_subject",
        trn."reporting_week_end_date",
        trn."domo_y_n_subject",
        SUM(CASE WHEN trn."impact" = 'High' THEN COALESCE(trn."plr_beds_preleased", 0) ELSE 0 END) AS "DirectMarketBedsPreleased",
        SUM(CASE WHEN trn."impact" = 'High' THEN COALESCE(CASE WHEN trn."classification" = 'Cardinal Community' THEN trn."total_marketed_units" ELSE trn."beds" END, 0) ELSE 0 END) AS "TotalDirectMarketBeds",
        SUM(CASE WHEN trn."impact" <> 'Subject Community' THEN COALESCE(trn."plr_beds_preleased", 0) ELSE 0 END) AS "TotalMarketBedsPreleased",
        SUM(CASE WHEN trn."impact" <> 'Subject Community' THEN COALESCE(CASE WHEN trn."classification" = 'Cardinal Community' THEN trn."total_marketed_units" ELSE trn."beds" END, 0) ELSE 0 END) AS "TotalMarketBeds"
    FROM "gcp_terrain_psql_reporting_trn_plr_occupancy_vw" trn
    GROUP BY 1, 2, 3
),
cy AS (
    SELECT 
        "AcademicYear",
        "AcademicYearBeginDate",
        "AcademicYearEndDate",
        "Date",
        "DayName",
        "WeekBeginDate",
        "WeekEndDate",
        "PropertyCd",
        "CommunityName",
        "CommunityGroupName",
        "TotalCommunityBeds",
        "PropertyType",
        "CGCellId",
        "CGGroupId",
        "OwnershipEntityName",
        "CGMgmtStatus",
        "EquityPartnerName",
        "Partner",
        "MgmtOpCoName",
        "KeystoneReportingInd",
        "PortfolioSlsMgrName",
        CONVERT_TIMEZONE('US/Mountain', "_BATCH_LAST_RUN_")::DATE AS "LastUpdatedOn",
        AVG("GoalLeasedBedCnt") AS "GoalLeasedBedCnt",
        AVG("GoalLeasedBedCntAYTD") AS "GoalLeasedBedCntAYTD",
        SUM(COALESCE("GrossLeasedBedSpaceCnt",0)) AS "GrossLeasedBedSpaceCnt",
        SUM(COALESCE("NetLeasedBedSpaceCnt",0)) AS "NetLeasedBedSpaceCnt",
        SUM(COALESCE("NetNewLeasedBedSpaceCnt",0)) AS "NetNewLeasedBedSpaceCnt",
        SUM(COALESCE("NetRenewalStayLeasedBedSpaceCnt",0)) AS "NetRenewalStayLeasedBedSpaceCnt",
        SUM(COALESCE("NetRenewalTransferLeasedBedSpaceCnt",0)) AS "NetRenewalTransferLeasedBedSpaceCnt"
    FROM "cumulative_community_pre_leasing_summary_student"
    GROUP BY 
        "AcademicYear",
        "AcademicYearBeginDate",
        "AcademicYearEndDate",
        "Date",
        "DayName",
        "WeekBeginDate",
        "WeekEndDate",
        "PropertyCd",
        "CommunityName",
        "CommunityGroupName",
        "TotalCommunityBeds",
        "PropertyType",
        "CGCellId",
        "CGGroupId",
        "OwnershipEntityName",
        "CGMgmtStatus",
        "EquityPartnerName",
        "Partner",
        "MgmtOpCoName",
        "KeystoneReportingInd",
        "PortfolioSlsMgrName",
        "_BATCH_LAST_RUN_"
),
FinalData AS (
    SELECT 
        cy."AcademicYear",
        cy."AcademicYearBeginDate",
        cy."AcademicYearEndDate",
        cy."Date",
        cy."DayName",
        cy."WeekBeginDate",
        cy."WeekEndDate",
        cy."PropertyCd",
        cy."CommunityName",
        cy."CommunityGroupName",
        cy."TotalCommunityBeds",
        cy."PropertyType",
        cy."CGCellId",
        cy."CGGroupId",
        cy."OwnershipEntityName",
        cy."CGMgmtStatus",
        cy."EquityPartnerName",
        cy."Partner",
        cy."MgmtOpCoName",
        cy."KeystoneReportingInd",
        cy."PortfolioSlsMgrName",
        cy."LastUpdatedOn",
        op."OpenLeasingPositions",
        rt."WeekToDateNetLeasedBedSpaceCnt",
        py."NetLeasedBedSpaceCnt" AS "PriorYearNetLeasedBedSpaceCnt",
        py."GrossLeasedBedSpaceCnt" AS "PriorYearGrossLeasedBedSpaceCnt",
        cy."GoalLeasedBedCnt",
        cy."GoalLeasedBedCntAYTD",
        cy."GrossLeasedBedSpaceCnt",
        cy."NetLeasedBedSpaceCnt",
        cy."NetNewLeasedBedSpaceCnt",
        cy."NetRenewalStayLeasedBedSpaceCnt",
        cy."NetRenewalTransferLeasedBedSpaceCnt",
        trn_sub."DirectMarketBedsPreleased",
        trn_sub."TotalDirectMarketBeds",
        trn_sub."TotalMarketBedsPreleased",
        trn_sub."TotalMarketBeds",
        trn_sub_py."DirectMarketBedsPreleased" AS "PriorYearDirectMarketBedsPreleased",  
        trn_sub_py."TotalDirectMarketBeds" AS "PriorYearTotalDirectMarketBeds",      
        trn_sub_py."TotalMarketBedsPreleased" AS "PriorYearTotalMarketBedsPreleased",
        trn_sub_py."TotalMarketBeds" AS "PriorYearTotalMarketBeds",
        (cy."NetLeasedBedSpaceCnt" - py."NetLeasedBedSpaceCnt") / NULLIF(cy."TotalCommunityBeds", 0) AS "VariancetoPriorYear",
        (cy."NetLeasedBedSpaceCnt" - cy."GoalLeasedBedCntAYTD") / NULLIF(cy."TotalCommunityBeds", 0) AS "VariancetoGoal",
        (cy."NetLeasedBedSpaceCnt" / NULLIF(cy."TotalCommunityBeds", 0)) - (trn_sub."TotalMarketBedsPreleased" / NULLIF(trn_sub."TotalMarketBeds", 0)) AS "VariancetoMarket",
        (trn_sub."TotalMarketBedsPreleased" / NULLIF(trn_sub."TotalMarketBeds", 0)) - (trn_sub_py."TotalMarketBedsPreleased" / NULLIF(trn_sub_py."TotalMarketBeds", 0)) AS "YoYMarketAverage",
        ora."ora_score" AS "ORAScore",
        (
            CASE 
                WHEN (cy."NetLeasedBedSpaceCnt" - py."NetLeasedBedSpaceCnt") / NULLIF(cy."TotalCommunityBeds", 0) <= -0.1 THEN 'Redlight'
                WHEN (cy."NetLeasedBedSpaceCnt" - cy."GoalLeasedBedCntAYTD") / NULLIF(cy."TotalCommunityBeds", 0) <= -0.1 THEN 'Redlight'
                WHEN (cy."NetLeasedBedSpaceCnt" / NULLIF(cy."TotalCommunityBeds", 0)) - (trn_sub."TotalMarketBedsPreleased" / NULLIF(trn_sub."TotalMarketBeds", 0)) <= -0.1 THEN 'Redlight'
                WHEN (cy."NetLeasedBedSpaceCnt" - py."NetLeasedBedSpaceCnt") / NULLIF(cy."TotalCommunityBeds", 0) <= -0.05 THEN 'Yellowlight'
                WHEN (cy."NetLeasedBedSpaceCnt" - cy."GoalLeasedBedCntAYTD") / NULLIF(cy."TotalCommunityBeds", 0) <= -0.05 THEN 'Yellowlight'
                WHEN (cy."NetLeasedBedSpaceCnt" / NULLIF(cy."TotalCommunityBeds", 0)) - (trn_sub."TotalMarketBedsPreleased" / NULLIF(trn_sub."TotalMarketBeds", 0)) <= -0.05 THEN 'Yellowlight'
                ELSE 'Greenlight'
            END
        ) AS "Color Status"
    FROM cy
    LEFT JOIN openpositions op ON cy."Date" = op."op_date" AND cy."PropertyCd" = op."op_id"
    LEFT JOIN RunningTotals rt ON cy."PropertyCd" = rt."PropertyCd" AND cy."Date" = rt."Date"
    LEFT JOIN trn_sub ON cy."PropertyCd" = trn_sub."external_id_subject" AND cy."WeekEndDate" = trn_sub."reporting_week_end_date"
    LEFT JOIN trn_sub trn_sub_py ON cy."PropertyCd" = trn_sub_py."external_id_subject" AND cy."WeekEndDate" = trn_sub_py."reporting_week_end_date"+364
    LEFT JOIN cy py ON DATE_PART('day', cy."Date") = DATE_PART('day', py."Date") AND DATE_PART('week', cy."Date") = DATE_PART('week', py."Date") AND DATE_PART('year', cy."Date") = DATE_PART('year', py."Date")+1 AND cy."PropertyCd" = py."PropertyCd"
    LEFT JOIN "gcp_jturner_reporting_jt_scores_vw" ora ON cy."PropertyCd" = ora."external_id" AND LAST_DAY(DATEADD(month, -1, cy."Date")) = LAST_DAY(ora."recent_month")
),
StatusChanges AS (
    SELECT
        *,
        LAG("Color Status") OVER (PARTITION BY "PropertyCd", "AcademicYear" ORDER BY "Date") AS PrevStatus
    FROM FinalData
),
StatusChangeGroups AS (
    SELECT
        *,
        CASE 
            WHEN "Color Status" != PrevStatus OR PrevStatus IS NULL THEN 1
            ELSE 0 
        END AS IsStatusChanged
    FROM StatusChanges
),
DaysSinceLastChange AS (
    SELECT
        "PropertyCd",
        "AcademicYear",
        MAX("Date") AS LastChangedDate
    FROM StatusChangeGroups
    WHERE "IsStatusChanged" = 1
    GROUP BY "PropertyCd", "AcademicYear"
)
SELECT DISTINCT
    scg.*,
    dsc."LastChangedDate"
FROM StatusChangeGroups scg
LEFT JOIN DaysSinceLastChange dsc 
    ON scg."PropertyCd" = dsc."PropertyCd"
    AND scg."AcademicYear" = dsc."AcademicYear";