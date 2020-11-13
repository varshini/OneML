
SELECT *  FROM [dbo].vw_RecommendationCurrent WHERE recommendationid in (select distinct recommendationid from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%') 
SELECT *  FROM [dbo].vw_RecommendationAction WHERE recommendationid in (select distinct recommendationid from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%') 

SELECT * FROM [dbo].vw_RecommendationAction WHERE SalesCenter = 'Unassigned'

SELECT *  FROM [dbo].vw_RecommendationAction WHERE recommendationid = 3727936
select DISTINCT RecModelName from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%' AND IsMerged is null
select * from vw_SPL_UniqueRecommendations WHERE TPID = '7295467'

/*Reviewed or Actioned Recommendations*/
  SELECT distinct recommendationID
  FROM [dbo].vw_RecommendationAction
  WHERE recommendationid in (select distinct recommendationid from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%') 
  AND (FeedbackType = 1 OR FeedbackType = 3 OR FeedbackType = 4 OR FeedbackType IS NULL)  

/*Positive disposition rate */
select distinct recommendationid from dbo.vw_RecommendationAction
where ((FeedbackType = 1 or FeedbackType = 4 or FeedbackType is null) or (FeedbackType = 3 and FeedbackReasonCode = 1))
and recommendationid in (select distinct recommendationid from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%')

/* No of opportunities */
  SELECT distinct OpportunityID, FuzzyFlag, OpportunityStatus, OpportunityEstimatedValue, OpportunityActualValue, OpportunityCreatedDate, OpportunityStatus
  FROM [dbo].vw_RecommendationAction
  WHERE recommendationid in (select distinct recommendationid from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%' AND IsMerged is null)
  AND (OpportunityID is not null) AND (OpportunityCreatedDate >= '2020-02-10 00:00:00.000')  AND OpportunityStatus != 'Lost'

 /* Pipeline estimated value */
SELECT distinct OpportunityID, OpportunityEstimatedValue, OpportunityCreatedDate, OpportunityStatus
FROM [dbo].vw_RecommendationAction
WHERE recommendationid in (select distinct recommendationid from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%' AND IsMerged = 1)
AND (OpportunityID is not null)  AND OpportunityStatus != 'Lost' AND (OpportunityCreatedDate >= '2020-02-01 00:00:00.000') 

SELECT * FROM [dbo].vw_RecommendationAction WHERE OpportunityID = 'ca5342af-fc07-ea11-a811-000d3a8ccd0c' /*61M*/


 /* Worked recommendations */
  SELECT distinct recommendationID
FROM [dbo].vw_RecommendationAction
WHERE recommendationid in (select distinct recommendationid from vw_SPL_UniqueRecommendations where RecModelName like '%M365 Customer Add%')
  AND (OpportunityID is not null
  OR EngagementID is not null
  OR TaskStatus = 'Completed')



