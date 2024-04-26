WITH ExternalIDs
AS (
    SELECT DISTINCT external_id
        , "property_name"
    FROM "gcp_entrata_postgressql_reporting_wo_request_details_vw"
    WHERE "portfolio_community_status" = 'CURRENT'
        AND "property_type_name" = 'Student'
        AND "domo_y_n" = 'Y'
    )
    , Dates
AS (
    SELECT dt
    FROM date_dimension
    WHERE dt BETWEEN '2023-09-01' AND CURRENT_DATE - 1
    )
    , WorkOrders
AS (
    SELECT "wo_actual_start_date"
        , "external_id"
        , COUNT("wo_actual_start_date") AS "WorkOrders"
    FROM "gcp_entrata_postgressql_reporting_wo_request_details_vw"
    WHERE "wo_status" NOT IN ('Awaiting Vendor', 'Cancelled')
        AND "portfolio_community_status" = 'CURRENT'
        AND "property_type_name" = 'Student'
        AND "is_parent_wo_request_id" = 1
        AND "domo_y_n" = 'Y'
    GROUP BY 1
        , 2
    )
    , Leads
AS (
    SELECT "GuestCardCreatedDate"::DATE AS "Date"
        , "PropertyCd"
        , COUNT("GuestCardCreatedDate") AS "Leads"
    FROM "vwleadlifecycle"
    WHERE UPPER("CGMgmtStatus") = 'CURRENT'
        AND "PropertyType" = 'Student'
    GROUP BY 1
        , 2
    )
    , MaintenanceTMs
AS (
    SELECT DISTINCT "snapshot_date"
        , "external_id"
        , COUNT("emp_id") AS MaintenanceTMs
    FROM "ultipro_demographics_time_series"
    WHERE "job_family" = 'Maintenance'
        AND "employment_status" = 'Active'
    GROUP BY 1
        , 2
    )
    , LeasingTMs
AS (
    SELECT DISTINCT "snapshot_date"
        , "external_id"
        , COUNT("emp_id") AS LeasingTMs
    FROM "ultipro_demographics_time_series"
    WHERE "job_family" = 'Leasing'
        AND "employment_status" = 'Active'
    GROUP BY 1
        , 2
    )
    , CrossJ
AS (
    SELECT e.external_id
        , e."property_name"
        , d.dt
    FROM ExternalIDs e
    CROSS JOIN Dates d
    )
SELECT cj.dt AS "Date"
    , cj.external_id
    , cj.property_name
    , "MaintenanceTMs"
    , "LeasingTMs"
    , "WorkOrders"
    , "Leads"
FROM CrossJ cj
LEFT JOIN WorkOrders wo
    ON cj.external_id = wo.external_id
        AND cj.dt = wo."wo_actual_start_date"
LEFT JOIN Leads ll
    ON cj.external_id = ll."PropertyCd"
        AND cj.dt = ll."Date"
LEFT JOIN MaintenanceTMs mtm
    ON cj."external_id" = mtm."external_id"
        AND LAST_DAY(cj."dt") = LAST_DAY(mtm."snapshot_date")
LEFT JOIN LeasingTMs ltm
    ON cj."external_id" = ltm."external_id"
        AND LAST_DAY(cj."dt") = LAST_DAY(ltm."snapshot_date")
ORDER BY cj.dt DESC;
