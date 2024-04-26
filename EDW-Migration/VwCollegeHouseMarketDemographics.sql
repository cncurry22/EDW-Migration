WITH DistanceCalc
AS (
    SELECT sc.name AS College
        , sc.KEY AS schoolkey
        , prop.milesToClosestCampus
        , prop.name AS Community
        , prop."totalBeds"
        , prop.occupancy
        , prop.prelease
        , prop.latitude AS property_latitude
        , prop.longitude AS property_longitude
        , sc.latitude AS university_latitude
        , sc.longitude AS university_longitude
        , sc."onCampusHousing"
        , sc."offCampusHousing"
        , SQRT(POW(69.1 * (prop.latitude - sc.latitude), 2) + POW(69.1 * (prop.longitude - sc.longitude) * COS(sc.latitude / 57.3), 2)) AS distance_miles
    FROM "rpt_properties_cardinal" prop
    CROSS JOIN "rpt_schools_cardinal" sc
    )
    , collegeassignments
AS (
    SELECT College
        , AVG(milesToClosestCampus) AS collegeproximity
        , SUM(totalbeds) AS collegetotalpurposebuiltbeds
        , SUM("occupancy" * "totalBeds") AS "collegeoccupiedbeds"
        , SUM("prelease" * "totalBeds") AS "collegepreleasedbeds"
        , SUM("totalBeds" * "offCampusHousing") AS "collegetotalbedstracked"
        , SUM("totalBeds" * "onCampusHousing") AS "collegeoncampusbeds"
    FROM DistanceCalc
    WHERE ABS(distance_miles - milesToClosestCampus) <= .05
    GROUP BY College
    ORDER BY College
    )
    , enrollmenttrends
AS (
    SELECT en."enrollmentYear"
        , mkt."region"
        , mkt.STATE
        , mkt.city
        , sc."name" AS "collegename"
        , (en."undergradEnrollment" + en."graduateEnrollment") AS collegetotalenrollment
        , LAG(en.undergradEnrollment) OVER (
            PARTITION BY en.schoolKey ORDER BY en.enrollmentYear DESC
            ) + LAG(en.graduateEnrollment) OVER (
            PARTITION BY en.schoolKey ORDER BY en.enrollmentYear DESC
            ) AS PriorYearcollegetotalenrollment
        , LAG(en.fulltimeundergradEnrollment) OVER (
            PARTITION BY en.schoolKey ORDER BY en.enrollmentYear DESC
            ) AS PriorYearfulltimeUndergradEnrollment
        , LAG(en.parttimeundergradEnrollment) OVER (
            PARTITION BY en.schoolKey ORDER BY en.enrollmentYear DESC
            ) AS PriorYearparttimeUndergradEnrollment
        , LAG(en."fullTimeGraduateEnrollment") OVER (
            PARTITION BY en.schoolKey ORDER BY en.enrollmentYear DESC
            ) AS PriorYearfullTimeGraduateEnrollment
        , LAG(en."partTimeGraduateEnrollment") OVER (
            PARTITION BY en.schoolKey ORDER BY en.enrollmentYear DESC
            ) AS PriorYearpartTimeGraduateEnrollment
        , (en."fullTimeUndergradEnrollment" + en."fullTimeGraduateEnrollment") AS collegefulltimeenrollment
        , (en."partTimeUndergradEnrollment" + en."partTimeGraduateEnrollment") AS collegeparttimeenrollment
        , mkt.bedsOnCampus AS marketbedsoncampus
        , mkt.bedsPurposeBuilt AS marketbedspurposebuilt
        , mkt.bedsleaseup AS marketbedsleaseup
        , mkt.bedsunderconstruction AS marketbedsunderconstruction
        , mkt.bedsplanned AS marketbedsplanned
        , mkt.totalpipelinebeds AS markettotalpipelinebeds
        , mkt.fulltimeenrollment AS marketfulltimeenrollment
        , mkt.parttimeenrollment AS marketparttimeenrollment
        , mkt.totalenrollment AS markettotalenrollment
        , sc."onCampusHousing" AS collegeoncampushousing
        , sc."offCampusHousing" AS collegeoffcampushousing
        , (en."undergradEnrollment" + en."graduateEnrollment") - (en."partTimeUndergradEnrollment" + en."partTimeGraduateEnrollment") - ca.collegeoncampusbeds - ca.collegepreleasedbeds AS "CurrentCollegeAvailability"
        , ca.collegeproximity
        , ca."collegeoccupiedbeds"
        , ca."collegepreleasedbeds"
        , ca."collegetotalpurposebuiltbeds"
        , ca."collegetotalbedstracked"
    FROM "rpt_enrollments_cardinal" en
    INNER JOIN "rpt_schools_cardinal" sc
        ON en."schoolKey" = sc."key"
    INNER JOIN "rpt_markets_cardinal" mkt
        ON sc."marketKey" = mkt."key"
    INNER JOIN collegeassignments ca
        ON sc.name = ca.college
    )
SELECT enrollmentyear
    , region
    , STATE
    , city
    , collegename
    , collegetotalenrollment
    , (collegetotalenrollment - PriorYearcollegetotalenrollment) / NULLIF(PriorYearcollegetotalenrollment, 0) AS YoYTotalEnrollmentChange
    , PriorYearcollegetotalenrollment
    , PriorYearfulltimeUndergradEnrollment
    , PriorYearparttimeUndergradEnrollment
    , PriorYearfullTimeGraduateEnrollment
    , PriorYearpartTimeGraduateEnrollment
    , (collegefulltimeenrollment - (PriorYearfullTimeGraduateEnrollment + PriorYearfulltimeUndergradEnrollment)) / NULLIF((PriorYearfullTimeGraduateEnrollment + PriorYearfulltimeUndergradEnrollment), 0) AS YoYTargetPopulationChange
    , collegeparttimeenrollment
    , marketbedsoncampus
    , marketbedspurposebuilt
    , marketbedsleaseup
    , marketbedsunderconstruction
    , marketbedsplanned
    , markettotalpipelinebeds
    , marketfulltimeenrollment
    , marketparttimeenrollment
    , markettotalenrollment
    , collegeoncampushousing
    , collegeoffcampushousing
    , "CurrentCollegeAvailability"
    , collegeproximity
    , "collegeoccupiedbeds"
    , "collegepreleasedbeds"
    , "collegetotalpurposebuiltbeds"
    , "collegetotalbedstracked"
FROM enrollmenttrends
ORDER BY "enrollmentYear" DESC
    , "city" ASC
    , "collegename" ASC
