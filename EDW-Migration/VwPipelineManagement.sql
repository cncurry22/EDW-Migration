-- DROP VIEW "Rpt"."VwLeadPipelineMgmt"
CREATE OR REPLACE VIEW "Rpt"."VwLeadPipelineMgmt" AS (
WITH ids
AS (
    SELECT DISTINCT "PropertyCd"
        , "CommunityName"
        , "CommunityBKey"
    FROM "DDM"."DimCommunity"
    WHERE "PropertyType" = 'Student'
        AND "KeystoneReportingInd"
        AND "CGMgmtStatus" = 'Current'
        AND "CurrentRowInd"
    )
    , dates
AS (
    SELECT "Date"
        , "WeekBeginDate"
        , "WeekEndDate"
        , CONCAT (
            SPLIT_PART("AcademicYear", '/', 1)::INTEGER + 1
            , '/'
            , SPLIT_PART("AcademicYear", '/', 2)::INTEGER + 1
            ) AS "AcademicYear"
        , "AcademicYearBeginDate"
    FROM "DDM"."DimDate"
    WHERE "Date" BETWEEN CURRENT_DATE - INTERVAL '2 YEAR' AND (CURRENT_DATE AT TIME ZONE 'US/Mountain')::DATE - INTERVAL '1 DAY'
         AND "CurrentRowInd"
    )
    , cj
AS (
    SELECT dd."Date"
        , dd."WeekBeginDate"
        , dd."WeekEndDate"
        , dd."AcademicYear"
        , dd."AcademicYearBeginDate"
        , dc."PropertyCd"
        , dc."CommunityName"
        , dc."CommunityBKey"
    FROM dates dd
    CROSS JOIN ids dc
    )
    , leads
AS (
    SELECT dates."GuestCardCreatedDate"::DATE
        , dc."CommunityName"
        , dc."PropertyCd"
        , COUNT(dates."GuestCardCreatedDate") AS "NewLeads"
        , COUNT(dates."LeaseApprovalDate") AS "NewLeases"
    FROM "DDM"."FctLeaseASnap" fct
    INNER JOIN (
        SELECT dc.*
            , COALESCE(cdc."PortfolioMgrName", dc."PortfolioMgrName") AS "CurrentPortfolioMgrName"
            , COALESCE(cdc."PortfolioSlsMgrName", dc."PortfolioSlsMgrName") AS "CurrentPortfolioSlsMgrName"
        FROM "DDM"."DimCommunity" dc
        LEFT JOIN "DDM"."DimCommunity" cdc
            ON dc."CommunityBKey" = cdc."CommunityBKey"
                AND cdc."CurrentRowInd"
        ) dc
        ON fct."CommunitySKey" = dc."CommunitySKey"
    INNER JOIN (
        SELECT fct."LeaseIntervalNKey"
            , NULLIF(ddgc."Date", '9999-12-31') + dtodgc."TimeOfDay24Hr" AS "GuestCardCreatedDate"
            , NULLIF(ddlap."Date", '9999-12-31') + dtodlap."TimeOfDay24Hr" AS "LeaseApprovalDate"
        FROM "DDM"."FctLeaseASnap" fct
        INNER JOIN "DDM"."DimDate" ddgc
            ON fct."GuestCardCreatedDateIKey" = ddgc."DateIKey"
        INNER JOIN "DDM"."DimDate" ddlap
            ON fct."LeaseApprovedDateIKey" = ddlap."DateIKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodgc
            ON fct."GuestCardCreatedTimeOfDaySKey" = dtodgc."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodlap
            ON fct."LeaseApprovedTimeOfDaySKey" = dtodlap."TimeOfDaySKey"
        ) dates
        ON dates."LeaseIntervalNKey" = fct."LeaseIntervalNKey"
    GROUP BY dates."GuestCardCreatedDate"::DATE
        , dc."CommunityName"
        , dc."PropertyCd"
    )
    , goals
AS (
    SELECT CAST(SPLIT_PART(dlg."LeaseGoalBKey", '|', 1) AS DATE) AS "Date"
        , SPLIT_PART(dlg."LeaseGoalBKey", '|', 2) AS "BKey"
        , dlg."CumulativeLeaseCnt" AS "Goal"
        , flg."AcademicYearEndGoal"
    FROM "DDM"."DimLeaseGoal" dlg
    INNER JOIN (
        SELECT SPLIT_PART("LeaseGoalBKey", '|', 2) AS "BKey"
            , "CumulativeLeaseCnt" AS "AcademicYearEndGoal"
        FROM "DDM"."DimLeaseGoal"
        WHERE SPLIT_PART("LeaseGoalBKey", '|', 1) = '20240908'
            AND "CurrentRowInd"
    ) flg ON SPLIT_PART(dlg."LeaseGoalBKey", '|', 2) = flg."BKey"
    WHERE "CurrentRowInd"
        AND "LeaseGoalBKey" != 'N/A'
    )
    , leasing
AS (
    SELECT dd."Date"
        , dc."PropertyCd"
        , COALESCE(SUM(ren."RenewalStay" + ren."RenewalTransfer") / SUM(ren."TotalLeases"),0.35) AS "AcademicYearEndRenewalPercentage"
        , SUM(fct."NetPreleasedBedSpaceCnt") AS "NetLeasedBedSpaceCnt"
    FROM "DDM"."FctCommunityPreleasingPSnap" fct
    INNER JOIN "DDM"."DimDate" dd
        ON fct."DateIKey" = dd."DateIKey"
    INNER JOIN "DDM"."DimCommunity" dc
        ON fct."CommunitySKey" = dc."CommunitySKey"
    INNER JOIN (
        SELECT dc."PropertyCd"
            , SUM(COALESCE(fct."NetRenewalStayPreleasedBedSpaceCnt",0)) AS "RenewalStay"
            , SUM(COALESCE(fct."NetRenewalTransferPreleasedBedSpaceCnt",0)) AS "RenewalTransfer"
            , SUM(NULLIF(fct."NetPreleasedBedSpaceCnt",0)) AS "TotalLeases" 
            -- , SUM(COALESCE(fct."NetRenewalStayPreleasedBedSpaceCnt",0)) + SUM(COALESCE(fct."NetRenewalTransferPreleasedBedSpaceCnt",0)) AS "RenewalPercentage"
            --  / SUM(NULLIF(fct."NetPreleasedBedSpaceCnt",0)) AS "RenewalPercentage"
        FROM "DDM"."FctCommunityPreleasingPSnap" fct
        INNER JOIN "DDM"."DimDate" dd
            ON fct."DateIKey" = dd."DateIKey"
        INNER JOIN "DDM"."DimCommunity" dc
            ON fct."CommunitySKey" = dc."CommunitySKey"
        WHERE dd."Date" = '20230908'
        GROUP BY 1
    ) ren ON dc."PropertyCd" = ren."PropertyCd"
    GROUP BY 1
        , 2
    )
SELECT cj."Date"
    , cj."WeekBeginDate"
    , cj."WeekEndDate"
    , cj."AcademicYear"
    -- , cj."AcademicYearBeginDate"
    , cj."PropertyCd"
    , cj."CommunityName"
    , cg."Goal" AS "CurrentGoal"
    , fg."Goal" AS "FutureGoal"
    , la."NetLeasedBedSpaceCnt"
    , cg."AcademicYearEndGoal"
    , la."AcademicYearEndRenewalPercentage"
    , ((fg."Goal" - la."NetLeasedBedSpaceCnt") * (1 - la."AcademicYearEndRenewalPercentage")) * (1/NULLIF((
        SUM(ll."NewLeases") OVER (
            PARTITION BY cj."PropertyCd" ORDER BY cj."Date" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING
            ) / NULLIF(SUM(ll."NewLeads") OVER (
                PARTITION BY cj."PropertyCd" ORDER BY cj."Date" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING
                ), 0)
        ),0)) AS "AdaptiveWeeklyLeadGoal"
    , ((fg."Goal" - cg."Goal") * (1 - la."AcademicYearEndRenewalPercentage")) * 4 AS "WeeklyLeadGoal"
    , ll."NewLeads"
    , ll."NewLeases"
    , lb."NewLeads" AS "PriorYearNewLeads"
    , lb."NewLeases" AS "PriorYearNewLeases"
    , SUM(ll."NewLeads") OVER (
        PARTITION BY cj."PropertyCd", cj."WeekBeginDate" ORDER BY cj."Date" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "WeektoDateLeads"
    , SUM(ll."NewLeases") OVER (
        PARTITION BY cj."PropertyCd", cj."WeekBeginDate" ORDER BY cj."Date" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "WeektoDateLeases"
    , SUM(ll."NewLeads") OVER (
        PARTITION BY cj."PropertyCd" ORDER BY cj."Date" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING
        ) AS "Trailing30DayLeads"
    , SUM(ll."NewLeases") OVER (
        PARTITION BY cj."PropertyCd" ORDER BY cj."Date" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING
        ) AS "Trailing30DayLeases"
    , SUM(ll."NewLeases") OVER (
        PARTITION BY cj."PropertyCd" ORDER BY cj."Date" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING
        ) / NULLIF(SUM(ll."NewLeads") OVER (
            PARTITION BY cj."PropertyCd" ORDER BY cj."Date" RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING
            ), 0) AS "Trailing30DayConversionPercent"
FROM cj
LEFT JOIN leads ll
    ON cj."Date" = ll."GuestCardCreatedDate"::DATE
        AND cj."PropertyCd" = ll."PropertyCd"
LEFT JOIN leads lb
    ON EXTRACT(DOW FROM cj."Date") = EXTRACT(DOW FROM ll."GuestCardCreatedDate"::DATE)
        AND EXTRACT(WEEK FROM cj."Date") = EXTRACT(WEEK FROM ll."GuestCardCreatedDate"::DATE)
        AND EXTRACT(MONTH FROM cj."Date") = EXTRACT(MONTH FROM ll."GuestCardCreatedDate"::DATE)
        AND EXTRACT(YEAR FROM cj."Date") = EXTRACT(YEAR FROM ll."GuestCardCreatedDate"::DATE) + 1
        AND cj."PropertyCd" = ll."PropertyCd"
LEFT JOIN goals fg
    ON cj."Date" = fg."Date" - 28
        AND cj."CommunityBKey" = fg."BKey"
LEFT JOIN goals cg
    ON cj."Date" = cg."Date"
        AND cj."CommunityBKey" = cg."BKey"
LEFT JOIN leasing la
    ON cj."Date" = la."Date"
        AND cj."PropertyCd" = la."PropertyCd"
-- WHERE cj."CommunityName" = 'Arena District'
ORDER BY cj."Date" DESC, cj."CommunityName" DESC
LIMIT 100;
    );
    ALTER TABLE "Rpt"."VwLeadPipelineMgmt"
        OWNER TO "Deployment";


