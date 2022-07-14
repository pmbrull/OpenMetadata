UPDATE dashboard_service_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.username'),
        '$.connection.config.clientId',
        JSON_EXTRACT(json, '$.connection.config.username')
    )
WHERE serviceType = 'Looker';

UPDATE dashboard_service_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.password'),
        '$.connection.config.clientSecret',
        JSON_EXTRACT(json, '$.connection.config.password')
    )
WHERE serviceType = 'Looker';

UPDATE dashboard_service_entity
SET json = JSON_REMOVE(json, '$.connection.config.env')
WHERE serviceType = 'Looker';
