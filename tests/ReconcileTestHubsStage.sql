{% test ReconcileTestHubsStage(model, BK, HK, stage) %}

WITH HubTableTests AS (
    SELECT
      '{{ model }}' AS TableName,
      'ReconcileHashAndBusinessKeys' AS TestName,
      NULL AS KeyValue,
      NULL AS DuplicateCount,
      (SELECT COUNT(*)
       FROM "{{env_var('DBT_EDM_TARGET_DATABASE')}}"."{{env_var('DBT_EDM_TARGET_SCHEMA')}}".{{ stage }}
       WHERE ({{ HK }}, {{ BK }}) NOT IN (SELECT {{ HK }}, {{ BK }} FROM {{ model }})) AS MissingCount
    FROM {{ model }}
    GROUP BY 1, 2, 3
    HAVING MissingCount > 0
)
SELECT *
FROM HubTableTests

{% endtest %}