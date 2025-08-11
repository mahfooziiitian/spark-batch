DESCRIBE EXTENDED system.access.audit;
select DISTINCT service_name
from system.access.audit;
select *
from system.access.audit
where service_name = 'clusterPolicies'
and action_name = 'create'
limit 100;