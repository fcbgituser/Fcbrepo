{% test ReconcileTestHubsStage(model, BK, HK, stage) %}

WITH HubTableTests AS (
    SELECT
      '{{ model }}' AS TableName,
      'ReconcileHashAndBusinessKeys' AS TestName,
      NULL AS KeyValue,
      NULL AS DuplicateCount,
      (SELECT COUNT(*)
       FROM "{{env_var('DBT_FCB_SOURCE_DATABASE')}}"."{{var('source_sch_fcb')}}".{{ stage }}
       WHERE ({{ HK }}, {{ BK }}) NOT IN (SELECT {{ HK }}, {{ BK }} FROM {{ model }})) AS MissingCount
    FROM {{ model }}
    GROUP BY 1, 2, 3
    HAVING MissingCount > 0
)
SELECT *
FROM HubTableTests

{% endtest %}