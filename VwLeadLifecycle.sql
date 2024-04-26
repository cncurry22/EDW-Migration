-- DROP VIEW "Rpt"."VwLeadLifecycle"
CREATE OR REPLACE VIEW "Rpt"."VwLeadLifecycle" AS (
    SELECT
        dc."CommunityName",
        dc."PropertyCd",
        dc."CurrentPortfolioMgrName" AS "PortfolioMgrName",
        dc."CurrentPortfolioSlsMgrName" AS "PortfolioSlsMgrName",
        dc."KeystoneReportingInd",
        dc."CGMgmtStatus",
        dc."PropertyType",
        dc."PropertyBuildingStyle",
        dc."PetFriendlyPolicyInd",
        dfp."FloorplanName",
        dpu."UnitNbr",
        dbs."BedSpaceNbr",
        CASE
            WHEN dlt."NewLeaseInd" THEN 'New'
            WHEN dlt."RenewalLeaseInd" THEN 'Renewal Stay'
            WHEN dlt."TransferLeaseInd" THEN 'Renewal Transfer'
            WHEN dlt."MonthToMonthLeaseInd" THEN 'Month-to-Month'
        END AS "LeaseType",
        CONCAT(dr."LastName", ', ', dr."FirstName") AS "ResidentName",
        NULLIF(GREATEST(
            COALESCE(dates."CancellationDate", '1900-01-01 00:00:00'),
            COALESCE(dates."GuestCardCreatedDate", '1900-01-01 00:00:00'),
            COALESCE(dates."TourDate", '1900-01-01 00:00:00'),
            COALESCE(dates."ApplicationStartedDate", '1900-01-01 00:00:00'),
            COALESCE(dates."ApplicationCompletedDate", '1900-01-01 00:00:00'),
            COALESCE(dates."ApplicationApprovalDate", '1900-01-01 00:00:00'),
            COALESCE(dates."LeaseStartedDate", '1900-01-01 00:00:00'),
            COALESCE(dates."LeaseCompletedDate", '1900-01-01 00:00:00'),
            COALESCE(dates."LeaseApprovalDate", '1900-01-01 00:00:00')
        ), '1900-01-01 00:00:00') AS "LastActivityDate",
        COALESCE(
            CASE
                WHEN dates."CancellationDate" IS NOT NULL AND dates."CancellationDate" >= GREATEST(
                    COALESCE(dates."GuestCardCreatedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."TourDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."ApplicationStartedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."ApplicationCompletedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."ApplicationApprovalDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."LeaseStartedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."LeaseCompletedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."LeaseApprovalDate", '1900-01-01 00:00:00')
                ) THEN 'Cancelled'
                WHEN dates."LeaseApprovalDate" IS NOT NULL THEN 'Lease Approved'
                WHEN dates."LeaseCompletedDate" IS NOT NULL THEN 'Lease Completed'
                WHEN dates."LeaseStartedDate" IS NOT NULL THEN 'Lease Started'
                WHEN dates."ApplicationApprovalDate" IS NOT NULL THEN 'Application Approved'
                WHEN dates."ApplicationCompletedDate" IS NOT NULL THEN 'Application Completed'
                WHEN dates."ApplicationStartedDate" IS NOT NULL THEN 'Application Started'
                WHEN dates."TourDate" IS NOT NULL THEN 'First Visit/Tour'
                WHEN dates."GuestCardCreatedDate" IS NOT NULL THEN 'Guest Card Created'
            ELSE NULL
        END) AS "LastMilestone",
        CASE
            WHEN dates."LeaseApprovalDate" IS NOT NULL AND dates."LeaseApprovalDate" > dates."CancellationDate" THEN NULL
            ELSE dates."CancellationDate"
        END AS "CancellationDate",
        dates."GuestCardCreatedDate",
        dates."TourDate",
        COALESCE(
            dates."ApplicationStartedDate",
            dates."ApplicationCompletedDate",
            dates."ApplicationApprovalDate",
            dates."LeaseStartedDate"
        ) AS "ApplicationStartedDate",
        COALESCE(
            dates."ApplicationCompletedDate",
            dates."ApplicationApprovalDate",
            dates."LeaseStartedDate",
            dates."LeaseCompletedDate",
            dates."LeaseApprovalDate"
        ) AS "ApplicationCompletedDate",
        COALESCE(
            dates."ApplicationApprovalDate",
            dates."LeaseStartedDate",
            dates."LeaseCompletedDate",
            dates."LeaseApprovalDate"
        ) AS "ApplicationApprovalDate",
        COALESCE(
            dates."LeaseStartedDate",
            dates."LeaseCompletedDate",
            dates."LeaseApprovalDate"
        ) AS "LeaseStartedDate",
        COALESCE(
            dates."LeaseCompletedDate",
            dates."LeaseApprovalDate"
        ) AS "LeaseCompletedDate",
        dates."LeaseApprovalDate",
        fct."NeglectedLeadInd",
        dates."FirstContactDate",
        dates."InitialFollowUpDate",
        dla."FullName" AS "LeasingAgent",
        dls."LeaseStatus",
        dols."LeadSourceName" AS "OriginatingLeadSrc",
        dcls."LeadSourceName" AS "ConvertingLeadSrc",
        dltrm."LeaseStartDate",
        dltrm."LeaseEndDate",
        dates."MoveInDate",
        dates."MoveOutDate",
        dltrm."TermDurationMonth" AS "TermDuration",
        CASE
            WHEN dltrm."BedSpaceCnt" IS NULL AND dates."LeaseApprovalDate" IS NOT NULL THEN 1
            ELSE dltrm."BedSpaceCnt"
        END AS "LeaseBedSpaceCnt",
        NULLIF(CAST(fct."LeaseNKey" AS INTEGER), -1) AS "LeaseID",
        fct."LeaseIntervalNKey"::INTEGER AS "LeaseIntervalID",
        fct."LeaseAppNKey"::INTEGER AS "ApplicationID"
    FROM
        "DDM"."FctLeaseASnap" fct
    INNER JOIN (
        SELECT
            dc.*,
            COALESCE(cdc."PortfolioMgrName", dc."PortfolioMgrName") AS "CurrentPortfolioMgrName",
            COALESCE(cdc."PortfolioSlsMgrName", dc."PortfolioSlsMgrName") AS "CurrentPortfolioSlsMgrName"
        FROM
            "DDM"."DimCommunity" dc
        LEFT JOIN "DDM"."DimCommunity" cdc ON dc."CommunityBKey" = cdc."CommunityBKey" AND cdc."CurrentRowInd"
    ) dc ON fct."CommunitySKey" = dc."CommunitySKey"
    INNER JOIN "DDM"."DimFloorplan" dfp ON fct."FloorplanSKey" = dfp."FloorplanSKey"
    INNER JOIN "DDM"."DimPropertyUnit" dpu ON fct."PropertyUnitSKey" = dpu."PropertyUnitSKey"
    INNER JOIN "DDM"."DimBedSpace" dbs ON fct."BedSpaceSKey" = dbs."BedSpaceSKey"
    INNER JOIN "DDM"."DimResident" dr ON fct."ResidentSKey" = dr."ResidentSKey"
    INNER JOIN "DDM"."DimLeaseAgent" dla ON fct."LeaseAgentSKey" = dla."LeaseAgentSKey"
    INNER JOIN "DDM"."DimLeadSource" dols ON fct."OriginatingLeadSourceSKey" = dols."LeadSourceSKey"
    INNER JOIN "DDM"."DimLeadSource" dcls ON fct."ConvertingLeadSourceSKey" = dcls."LeadSourceSKey"
    INNER JOIN "DDM"."DimLeaseType" dlt ON fct."LeaseTypeSKey" = dlt."LeaseTypeSKey"
    INNER JOIN "DDM"."DimLeaseStatus" dls ON fct."LeaseStatusSKey" = dls."LeaseStatusSKey"
    INNER JOIN "DDM"."DimLeaseAppStg" dlas ON fct."LeaseAppStgSKey" = dlas."LeaseAppStgSKey"
    INNER JOIN "DDM"."DimLeaseTerm" dltrm ON fct."LeaseTermSKey" = dltrm."LeaseTermSKey"
    INNER JOIN (
        SELECT
            fct."LeaseIntervalNKey",
            NULLIF(ddgc."Date", '9999-12-31') + dtodgc."TimeOfDay24Hr" AS "GuestCardCreatedDate",
            NULLIF(ddfc."Date", '9999-12-31') + dtodfc."TimeOfDay24Hr" AS "FirstContactDate",
            NULLIF(ddic."Date", '9999-12-31') + dtodic."TimeOfDay24Hr" AS "InitialFollowUpDate",
            NULLIF(ddtour."Date", '9999-12-31') + dtodtour."TimeOfDay24Hr" AS "TourDate",
            NULLIF(ddaps."Date", '9999-12-31') + dtodaps."TimeOfDay24Hr" AS "ApplicationStartedDate",
            NULLIF(ddapc."Date", '9999-12-31') + dtodapc."TimeOfDay24Hr" AS "ApplicationCompletedDate",
            NULLIF(ddapa."Date", '9999-12-31') + dtodapa."TimeOfDay24Hr" AS "ApplicationApprovalDate",
            NULLIF(ddlas."Date", '9999-12-31') + dtodlas."TimeOfDay24Hr" AS "LeaseStartedDate",
            NULLIF(ddlac."Date", '9999-12-31') + dtodlac."TimeOfDay24Hr" AS "LeaseCompletedDate",
            NULLIF(ddlap."Date", '9999-12-31') + dtodlap."TimeOfDay24Hr" AS "LeaseApprovalDate",
            NULLIF(ddcxl."Date", '9999-12-31') + dtodcxl."TimeOfDay24Hr" AS "CancellationDate",
            NULLIF(ddmoi."Date", '9999-12-31') + dtodmoi."TimeOfDay24Hr" AS "MoveInDate",
            NULLIF(ddmou."Date", '9999-12-31') + dtodmou."TimeOfDay24Hr" AS "MoveOutDate"
        FROM
            "DDM"."FctLeaseASnap" fct
        INNER JOIN "DDM"."DimDate" ddgc ON fct."GuestCardCreatedDateIKey" = ddgc."DateIKey"
        INNER JOIN "DDM"."DimDate" ddfc ON fct."FirstContactDateIKey" = ddfc."DateIKey"
        INNER JOIN "DDM"."DimDate" ddic ON fct."InitialFollowUpDateIKey" = ddic."DateIKey"
        INNER JOIN "DDM"."DimDate" ddtour ON fct."TourCompletedDateIKey" = ddtour."DateIKey"
        INNER JOIN "DDM"."DimDate" ddaps ON fct."AppStartedDateIKey" = ddaps."DateIKey"
        INNER JOIN "DDM"."DimDate" ddapc ON fct."AppCompletedDateIKey" = ddapc."DateIKey"
        INNER JOIN "DDM"."DimDate" ddapa ON fct."AppApprovedDateIKey" = ddapa."DateIKey"
        INNER JOIN "DDM"."DimDate" ddlas ON fct."LeaseStartedDateIKey" = ddlas."DateIKey"
        INNER JOIN "DDM"."DimDate" ddlac ON fct."LeaseCompletedDateIKey" = ddlac."DateIKey"
        INNER JOIN "DDM"."DimDate" ddlap ON fct."LeaseApprovedDateIKey" = ddlap."DateIKey"
        INNER JOIN "DDM"."DimDate" ddcxl ON fct."CxlDateIKey" = ddcxl."DateIKey"
        INNER JOIN "DDM"."DimDate" ddmoi ON fct."MoveInDateIKey" = ddmoi."DateIKey"
        INNER JOIN "DDM"."DimDate" ddmou ON fct."MoveOutDateIKey" = ddmou."DateIKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodgc ON fct."GuestCardCreatedTimeOfDaySKey" = dtodgc."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodfc ON fct."FirstContactTimeOfDaySKey" = dtodfc."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodic ON fct."InitialFollowUpTimeOfDaySKey" = dtodic."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodtour ON fct."TourCompletedTimeOfDaySKey" = dtodtour."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodaps ON fct."AppStartedTimeOfDaySKey" = dtodaps."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodapc ON fct."AppCompletedTimeOfDaySKey" = dtodapc."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodapa ON fct."AppApprovedTimeOfDaySKey" = dtodapa."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodlas ON fct."LeaseStartedTimeOfDaySKey" = dtodlas."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodlac ON fct."LeaseCompletedTimeOfDaySKey" = dtodlac."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodlap ON fct."LeaseApprovedTimeOfDaySKey" = dtodlap."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodcxl ON fct."CxlTimeOfDaySKey" = dtodcxl."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodmoi ON fct."MoveInTimeOfDaySKey" = dtodmoi."TimeOfDaySKey"
        INNER JOIN "DDM"."DimTimeOfDay" dtodmou ON fct."MoveOutTimeOfDaySKey" = dtodmou."TimeOfDaySKey"
    ) dates ON dates."LeaseIntervalNKey" = fct."LeaseIntervalNKey"
    WHERE NULLIF(GREATEST(
                    COALESCE(dates."CancellationDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."GuestCardCreatedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."TourDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."ApplicationStartedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."ApplicationCompletedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."ApplicationApprovalDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."LeaseStartedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."LeaseCompletedDate", '1900-01-01 00:00:00'),
                    COALESCE(dates."LeaseApprovalDate", '1900-01-01 00:00:00')
                    ), '1900-01-01 00:00:00') IS NOT NULL
        AND UPPER(dc."CGMgmtStatus") != 'PAST'
        AND dc."KeystoneReportingInd"
    ORDER BY
        "LastActivityDate" DESC
);

ALTER TABLE "Rpt"."VwLeadLifecycle"
    OWNER TO "Deployment";