WITH market
AS (
    SELECT DATE_TRUNC('week', pr.updated)::DATE AS "AsOfDate"
        , sc.name AS college
        , mkt."region"
        , mkt.STATE
        , mkt.city
        , pn.beds::INTEGER AS "beds"
        , AVG(prop.milesToClosestCampus) AS "marketproximity"
        , AVG(CASE 
                WHEN TRIM(prop.yearBuilt) ~ '^[0-9]+$'
                    THEN prop.yearBuilt::INTEGER
                ELSE 0
                END) AS "marketvintage"
        , SUM(pn.areaSF * pr.totalbeds) / NULLIF(SUM(pr.totalbeds), 0) AS "marketsqft"
        , SUM(pr.rate * pr.totalbeds) / NULLIF(SUM(pr.totalbeds), 0) AS "marketrate"
        , MIN(pr.rate) AS MinMarketRate
        , MAX(pr.rate) AS MaxMarketRate
        , SUM(pr.totalBeds) AS "totalmarketBeds"
        , SUM(pr.prelease * pr.totalbeds) / NULLIF(SUM(pr.totalbeds), 0) AS "marketprelease"
        , SUM(pr.occupancy * pr.totalbeds) / NULLIF(SUM(pr.totalbeds), 0) AS "marketoccupancy"
    FROM "rpt_plan_reports_cardinal" pr
    INNER JOIN "rpt_plans_cardinal" pn
        ON pr.planKey = pn.KEY
    INNER JOIN "rpt_properties_cardinal" prop
        ON pn.propertyKey = prop.KEY
    INNER JOIN "rpt_markets_cardinal" mkt
        ON prop.marketKey = mkt.KEY
    LEFT JOIN "collegehousemarketcompetitormapping" mp
        ON prop.KEY = mp."CH key"
    LEFT JOIN "gcp_terrain_psql_reporting_trn_community_vw" trn
        ON mp."Terrain Name" = trn."property_name_comp"
    -- LEFT JOIN 
    --  	ranked rnk ON prop.name = rnk.comp
    INNER JOIN "rpt_schools_cardinal" sc
        ON ABS(SQRT(POW(69.1 * (prop.latitude - sc.latitude), 2) + POW(69.1 * (prop.longitude - sc.longitude) * COS(sc.latitude / 57.3), 2))) - prop.milesToClosestCampus < 0.05
    WHERE pn."is_from_cardinal" = 0
    --	rnk.similarityrank <= 5
    --  pn."is_from_cardinal" = 0 AND trn."impact" = 'High'
    GROUP BY DATE_TRUNC('week', pr.updated)::DATE
        , college
        , mkt."region"
        , mkt.STATE
        , mkt.city
        , pn.beds
    )
    , subject
AS (
    SELECT DATE_TRUNC('week', pr.updated)::DATE AS "AsOfDate"
        , sc.name AS college
        , dd.AcademicYearBeginDate
        , dd.AcademicYearEndDate
        , sc.onCampusHousing AS collegeoncampushousing
        , sc.offCampusHousing AS collegeoffcampushousing
        , mkt."region"
        , mkt.STATE
        , mkt.city
        , prop.name AS "subjectcommunity"
        , pn.beds::INTEGER AS "beds"
        , pn.name AS "FloorplanName"
        , AVG(prop.milesToClosestCampus) AS "subjectproximity"
        , AVG(CASE 
                WHEN TRIM(prop.yearBuilt) ~ '^[0-9]+$'
                    THEN prop.yearBuilt::INTEGER
                ELSE 0
                END) AS "subjectvintage"
        , SUM(pn.areaSF * pr.totalbeds) / SUM(NULLIF(pr.totalbeds, 0)) AS "subjectsqft"
        , SUM(pr.rate * pr.totalbeds) / SUM(NULLIF(pr.totalbeds, 0)) AS "subjectaskingrate"
        , SUM(wrr."new_lease_total_rent") / SUM(NULLIF(wrr."new_leases", 0)) AS "subjectnewleaserate"
        , SUM(wrr."new_leases") AS "subjectweeklynewleases"
        , SUM(pr.totalBeds) AS "totalsubjectBeds"
        , SUM(pr.prelease * pr.totalbeds) / SUM(NULLIF(pr.totalbeds, 0)) AS "subjectprelease"
        , SUM(pr.occupancy * pr.totalbeds) / SUM(NULLIF(pr.totalbeds, 0)) AS "subjectoccupancy"
  		, COUNT(ll."GuestCardCreatedDate") AS "WeeklyLeads"
    FROM "rpt_plan_reports_cardinal" pr
    INNER JOIN "rpt_plans_cardinal" pn
        ON pr.planKey = pn.KEY
    INNER JOIN "rpt_properties_cardinal" prop
        ON pn.propertyKey = prop.KEY
    INNER JOIN "rpt_markets_cardinal" mkt
        ON prop.marketKey = mkt.KEY
    LEFT JOIN "collegehousemarketcompetitormapping" mp
        ON prop.KEY = mp."CH key"
    LEFT JOIN "gcp_terrain_psql_reporting_trn_community_vw" trn
        ON mp."Terrain Name" = trn."property_name_comp"
    INNER JOIN "date_dimension" dd
        ON dd."Date" = DATE_TRUNC('week', pr.updated)::DATE
    INNER JOIN "vwleadlifecycle" ll
        ON DATE_TRUNC('week', pr.updated)::DATE = DATE_TRUNC('week', ll."GuestCardCreatedDate")::DATE
          AND mp."Yardi ID" = ll."PropertyCd"
          AND pn."name" = ll."FloorplanName"
    INNER JOIN "rpt_schools_cardinal" sc
        ON ABS(SQRT(POW(69.1 * (prop.latitude - sc.latitude), 2) + POW(69.1 * (prop.longitude - sc.longitude) * COS(sc.latitude / 57.3), 2))) - prop.milesToClosestCampus < 0.05
    INNER JOIN (
        WITH RankedLeases AS (
                SELECT "week_end_date" AS week
                    , "external_id"
                    , floorplan_name AS floorplan
                    , COALESCE(SUM(new_leases), 0) AS new_leases
                    , COALESCE(SUM("new_lease_total_rent"), 0) AS new_lease_total_rent
                    , ROW_NUMBER() OVER (
                        PARTITION BY "external_id"
                        , floorplan ORDER BY week_end_date DESC
                        ) AS rn
                FROM "gcp_entrata_reporting_weekly_rental_rate_summary_vw"
                GROUP BY "week_end_date"
                    , "external_id"
                    , floorplan
                )
            , FallbackData AS (
                SELECT R.week
                    , R.external_id
                    , R.floorplan
                    , R.new_leases
                    , R.new_lease_total_rent
                    , MAX(CASE 
                            WHEN R.new_leases > 0
                                THEN R.new_leases
                            END) OVER (
                        PARTITION BY R.external_id
                        , R.floorplan ORDER BY R.week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS fallback_new_leases
                    , MAX(CASE 
                            WHEN R.new_lease_total_rent > 0
                                THEN R.new_lease_total_rent
                            END) OVER (
                        PARTITION BY R.external_id
                        , R.floorplan ORDER BY R.week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS fallback_new_lease_total_rent
                FROM RankedLeases R
                )
        SELECT F.week
            , F.external_id
            , F.floorplan
            , CASE 
                WHEN F.new_leases > 0
                    THEN F.new_leases
                ELSE F.fallback_new_leases
                END AS new_leases
            , CASE 
                WHEN F.new_lease_total_rent > 0
                    THEN F.new_lease_total_rent
                ELSE F.fallback_new_lease_total_rent
                END AS new_lease_total_rent
        FROM FallbackData F
        ) wrr
        ON DATE_TRUNC('week', pr.updated)::DATE = wrr."week"
            AND mp."Yardi ID" = wrr."external_id"
            AND pn.name = wrr."floorplan"
    WHERE pn."is_from_cardinal" = 1
    GROUP BY DATE_TRUNC('week', pr.updated)::DATE
        , college
        , dd.AcademicYearBeginDate
        , dd.AcademicYearEndDate
        , sc.onCampusHousing
        , sc.offCampusHousing
        , mkt."region"
        , mkt.STATE
        , mkt.city
        , "subjectcommunity"
        , pn.beds::INTEGER
        , pn.name
    )
    , crr
AS (
    SELECT "as_of_date"
        , "external_id"
        , "number_of_bedrooms"
        , "floorplan_name"
        , SUM("total_signed_monthly_rent") AS "YTDSignedRent"
  		, SUM("new_leases") AS "YTDNewLeases"
    FROM "gcp_entrata_reporting_cumulative_rental_rate_summary_vw"
    GROUP BY "as_of_date"
        , "external_id"
        , "number_of_bedrooms"
        , "floorplan_name"
    )
    , bd
AS (
    SELECT "as_of_date"
  		, "external_id"
        , SUM("bed_count") AS "TotalSubjectCommunityBeds"
    FROM "gcp_entrata_reporting_cumulative_rental_rate_summary_vw"
    GROUP BY 1, 2
    )
SELECT ss."AsOfDate"
    , ss.college
    , ss.AcademicYearBeginDate
    , ss.AcademicYearEndDate
    , DATEDIFF('week', ss.AsofDate, ss.AcademicYearEndDate) AS "WeeksLeftinAcademicYear"
    , ss.collegeoncampushousing
    , ss.collegeoffcampushousing
    , ss."region"
    , ss.STATE
    , ss.city
    , ss."subjectcommunity"
    , bd."TotalSubjectCommunityBeds"
    , bud."Target Occupancy"
    , bud."Target Rent Revenue"
    , crr."YTDSignedRent"
    , ss."beds"
    , ss.FloorplanName
    , mk."marketproximity"
    , mk."marketvintage"
    , mk."marketsqft"
    , mk."marketrate"
    , (
        mk."marketrate" - LAG(mk."marketrate") OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            )
        ) 
        /*
        / NULLIF(LAG(mk."marketrate") OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            ), 0) 
         */
         AS "WeeklyMarketRateChange"
    , (
        mk."marketrate" - LAG(mk."marketrate", 52) OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            ))
        /*
        / NULLIF(LAG(mk."marketrate", 52) OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            ), 0) 
         */   
            AS "YearlyMarketRateChange"
    , mk.MinMarketRate
    , mk.MaxMarketRate
    , mk."marketprelease"
    , (
        mk."marketprelease" - LAG(mk."marketprelease") OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            ))
      /*
        ) / NULLIF(LAG(mk."marketprelease") OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            ), 0) 
      */
      AS "WeeklyMarketPreleaseChange"
    , (
        mk."marketprelease" - LAG(mk."marketprelease", 52) OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            )
        ) 
        /*
        / NULLIF(LAG(mk."marketprelease", 52) OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            ), 0)
        */
        AS "YearlyMarketPreleaseChange"
    , (
        mk."marketoccupancy" - LAG(mk."marketoccupancy", 52) OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            )
        ) 
        /*
        / NULLIF(LAG(mk."marketoccupancy", 52) OVER (
            PARTITION BY ss.college
            , ss.beds ORDER BY ss."AsOfDate"
            ), 0) 
        */
        AS "YearlyMarketOccupancyChange"
    , mk."marketoccupancy"
    , ss."subjectproximity"
    , ss."subjectaskingrate"
    , (
        ss."subjectaskingrate" - LAG(ss."subjectaskingrate") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) 
        /*
        / NULLIF(LAG(ss."subjectaskingrate") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) 
         */
         AS "WeeklySubjectAskingRateChange"
            /*
    , (
        ss."subjectaskingrate" - LAG(ss."subjectaskingrate", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) / NULLIF(LAG(ss."subjectaskingrate", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) AS "YearlySubjectAskingRateChange"
            */
    , ss."subjectnewleaserate"
    , ss."subjectweeklynewleases"
    , (
        ss."subjectnewleaserate" - LAG(ss."subjectnewleaserate") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) 
        /*
        / NULLIF(LAG(ss."subjectnewleaserate") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) 
        */
        AS "WeeklySubjectNewLeaseRateChange"
            /*
    , (
        ss."subjectnewleaserate" - LAG(ss."subjectnewleaserate", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) / NULLIF(LAG(ss."subjectnewleaserate", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) AS "YearlySubjectNewRateChange"
            */
    , ss."subjectvintage"
--    , ss."subjectsqft"
    , ss."totalsubjectbeds"
    , ss."subjectprelease"
    , (
        ss."subjectprelease" - LAG(ss."subjectprelease") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) 
        /*
        / NULLIF(LAG(ss."subjectprelease") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) 
         */ AS "WeeklySubjectPreleaseChange"
            /*
    , (
        ss."subjectprelease" - LAG(ss."subjectprelease", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) / NULLIF(LAG(ss."subjectprelease", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) AS "YearlySubjectPreleaseChange"
            */
    , (
        ss."subjectweeklynewleases" - LAG(ss."subjectweeklynewleases") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) 
        /*
        / NULLIF(LAG(ss."subjectweeklynewleases") OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) 
          */ AS "WeeklySubjectNewLeaseChange"
            /*
    , (
        ss."subjectweeklynewleases" - LAG(ss."subjectweeklynewleases", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            )
        ) / NULLIF(LAG(ss."subjectweeklynewleases", 52) OVER (
            PARTITION BY ss.college
            , ss.subjectcommunity
            , ss.beds
            , ss.floorplanname ORDER BY ss."AsOfDate"
            ), 0) AS "YearlySubjectNewLeaseChange"
            */
     , ss."WeeklyLeads"
     , crrpy."YTDNewLeases" AS "SustainableNewLeaseCount"
FROM subject ss
LEFT JOIN market mk
    ON mk."AsOfDate" = ss."AsOfDate"
        AND LOWER(mk.college) = LOWER(ss.college)
        AND mk.beds = ss.beds
LEFT JOIN "collegehousemarketcompetitormapping" mp
    ON ss.subjectcommunity = mp."CH Name"
LEFT JOIN "rate_reccomendation_tool_budgeted_target_values" bud
    ON mp."Yardi ID" = bud."external_id"
LEFT JOIN crr
    ON mp."Yardi ID" = crr."external_id"
        AND crr."as_of_date" = ss."AsofDate"
        AND crr."number_of_bedrooms" = ss."beds"
        AND crr."floorplan_name" = ss."FloorplanName"
LEFT JOIN crr crrpy
    ON mp."Yardi ID" = crrpy."external_id"
        AND crrpy."as_of_date" = '2023-09-12'
        AND crrpy."number_of_bedrooms" = ss."beds"
        AND crrpy."floorplan_name" = ss."FloorplanName"
LEFT JOIN bd 
	ON mp."Yardi ID" = bd."external_id"
    	AND ss."AsofDate" = bd."as_of_date"
ORDER BY ss."AsOfDate" DESC
    , ss.city ASC
    , ss.beds DESC
    , ss."FloorplanName" ASC;
