WITH ids
AS (
    SELECT DISTINCT "PropertyCd", "CommunityName"
    FROM "cumulative_community_pre_leasing_summary_student"
    WHERE UPPER(PropertyType) = 'STUDENT'
        AND UPPER(KeystoneReportingInd) = 'TRUE'
        AND UPPER(CGMgmtStatus) = 'CURRENT'
    )
    , dates
AS (
    SELECT "Date"
  		, "AcademicYear"
  		, "WeekBeginDate"
  		, "WeekEndDate"
    FROM "date_dimension"
    WHERE "Date" BETWEEN '2023-09-18' AND CONVERT_TIMEZONE('US/Mountain', CURRENT_DATE)::DATE + 90
    )
    , cj
AS (
    SELECT ids."PropertyCd"
  		, ids."CommunityName"
        , dates."Date"
  		, dates."AcademicYear"
  		, dates."WeekBeginDate"
  		, dates."WeekEndDate"
    FROM ids
    CROSS JOIN dates
    )
    , leads
AS (
    SELECT DATE_TRUNC('day', "GuestCardCreatedDate")::DATE AS "GuestCardCreatedDate"
        , "PropertyCd"
        , "CommunityName"
        , COUNT("GuestCardCreatedDate") AS "NewLeads"
        , COUNT("LeaseApprovalDate") AS "LeasesApproved"
    FROM "vwleadlifecycle"
    WHERE "GuestCardCreatedDate" IS NOT NULL
    GROUP BY DATE_TRUNC('day', "GuestCardCreatedDate")::DATE
        , "PropertyCd"
        , "CommunityName"
    )
    , leasing
AS (
    SELECT "Date"
        , "PropertyCd"
        , SUM("NetLeasedBedSpaceCnt") AS "NetLeasedBedSpaceCnt"
        , AVG("TotalCommunityBeds") AS "TotalCommunityBeds"
        , AVG("GoalLeasedBedCntAYTD") AS "GoalLeasedBedCntAYTD"
    FROM "cumulative_community_pre_leasing_summary_student"
    GROUP BY 1
        , 2
    )
    , futuregoal
AS (
    SELECT "Date"
        , "PropertyCd"
        , AVG("GoalLeasedBedCntAYTD") AS "FutureGoal"
    FROM "cumulative_community_pre_leasing_summary_student"
    GROUP BY 1
        , 2
    )
    , renewal
AS (
    SELECT "Date"
        , "PropertyCd"
        , COALESCE(SUM(COALESCE("NetRenewalStayLeasedBedSpaceCnt", 0)::INTEGER + COALESCE("NetRenewalTransferLeasedBedSpaceCnt", 0)::INTEGER) / NULLIF(SUM("NetLeasedBedSpaceCnt"), 0), 0.35) AS "RenewalPercentage"
    FROM "cumulative_community_pre_leasing_summary_student"
    WHERE "Date" = '20230916'
    GROUP BY 1
        , 2
    )
    , py
AS (
    SELECT DATE_TRUNC('day', "GuestCardCreatedDate")::DATE AS "GuestCardCreatedDate"
        , "PropertyCd"
        , "CommunityName"
        , COUNT("GuestCardCreatedDate") AS "PriorYearNewLeads"
        , COUNT("LeaseApprovalDate") AS "PriorYearLeasesApproved"
    FROM "vwleadlifecycle"
    WHERE "GuestCardCreatedDate" IS NOT NULL
    GROUP BY DATE_TRUNC('day', "GuestCardCreatedDate")::DATE
        , "PropertyCd"
        , "CommunityName"
    )
SELECT cj."Date"
  	, cj."AcademicYear"
  	, cj."WeekBeginDate"
  	, cj."WeekEndDate"
    , cj."PropertyCd"
    , cj."CommunityName"
    , ren."RenewalPercentage"
    , COALESCE(la."NetLeasedBedSpaceCnt",0) AS "NetLeasedBedSpaceCnt"
    , la."TotalCommunityBeds"
    , COALESCE(ll."NewLeads",0) AS "NewLeads"
    , COALESCE(ll."LeasesApproved",0) AS "LeasesApproved"
    , COALESCE(py."PriorYearNewLeads",0) AS "PriorYearNewLeads"
    , COALESCE(py."PriorYearLeasesApproved",0) AS "PriorYearLeasesApproved"
    , la."GoalLeasedBedCntAYTD"
    , fu."FutureGoal"
FROM cj
LEFT JOIN leads ll
    ON cj."Date" = ll."GuestCardCreatedDate"::DATE
        AND cj."PropertyCd" = ll."PropertyCd"
LEFT JOIN leasing la
    ON cj."Date" = la."Date"
        AND cj."PropertyCd" = la."PropertyCd"
LEFT JOIN futuregoal fu
    ON cj."Date" = fu."Date" - 28
        AND cj."PropertyCd" = fu."PropertyCd"
LEFT JOIN renewal ren
    ON cj."PropertyCd" = ren."PropertyCd"
LEFT JOIN py 
	ON cj."Date" = py."GuestCardCreatedDate"::DATE + 364
    	AND cj."PropertyCd" = py."PropertyCd"
     
