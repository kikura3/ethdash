/* SQLs for ELT */
/* To be setup via DBT */

DROP TABLE dn_validators;

CREATE TABLE dn_validators AS
  SELECT
  block_number,
         block_time,
         Concat('0x', Substring(depositor FROM 3))             AS depositor,
         Concat('0x', Substring(validator FROM 3))             AS validator,
         Concat('0x', Substring(withdrawal_credential FROM 3)) AS
  withdrawal_credential
  FROM   dn_validators_legacy 


DROP TABLE bc_validator_info;

CREATE TABLE bc_validator_info AS
SELECT DISTINCT activationeligibilityepoch,
                activationepoch,
                balance,
                effectivebalance,
                exitepoch,
                lastattestationslot,
                name,
                pubkey,
                slashed,
                status,
                validatorindex,
                withdrawableepoch,
                withdrawalcredentials
FROM   validator_info;

CREATE UNIQUE INDEX bc_validator_info_unique_index  
ON bc_validator_info (validatorindex); 

CREATE TABLE blockprint_client AS
  SELECT proposer_index,
         predicted_client
  FROM   blockprint bp1
  WHERE  slot IN (SELECT Max(slot)
                  FROM   blockprint bp2
                  WHERE  bp2.proposer_index = bp1.proposer_index);

DROP TABLE ui_client_info;

CREATE TABLE ui_client_info AS
SELECT 
    user_agent_name   AS consensus_client,
    client_name       AS execution_client,
    count(*)          AS total_nodes,
    nw.last_updated   AS last_updated
FROM 
    nodewatch nw
    LEFT JOIN ethernodes en
        ON nw.ip = en.ip
GROUP BY 
    consensus_client,
    execution_client;

DROP TABLE ui_consensus_hosting_info;

CREATE TABLE ui_consensus_hosting_info AS
SELECT 
	nw.user_agent_name AS consensus_client,
	CASE WHEN ng.asn_name LIKE '%AMAZON%' THEN 'Amazon'
	     WHEN ng.asn_name LIKE '%GOOGLE%' THEN 'Google'
	     WHEN ng.asn_name LIKE '%ORACLE%' THEN 'Oracle'
	     WHEN ng.asn_name LIKE '%MICROSOFT%' THEN 'Microsoft'
	     WHEN UPPER(ng.asn_name) LIKE 'CONTABO%' THEN 'Contabo'
	     WHEN UPPER(ng.asn_name) LIKE 'HETZNER%' THEN 'Hetzner Online GmbH'
	     WHEN UPPER(ng.asn_name) LIKE 'OVH%' THEN 'OVH SAS'
	     WHEN ng.asn_name LIKE 'ATT%' THEN 'Others'
	     WHEN ng.asn_name LIKE 'COMCAST%' THEN 'Others'
	     WHEN ng.asn_name LIKE 'UUNET%' THEN 'Others'
	     WHEN ng.asn_name LIKE 'TWC%' THEN 'Others'
	     WHEN ng.asn_name LIKE 'ASN%' THEN 'Others'
	     WHEN ng.asn_name LIKE '%Telekom%' THEN 'Others'
	     WHEN ng.asn_name LIKE '%Free SAS%' THEN 'Others'
	     ELSE ng.asn_name END AS hosting_provider_name,
	COUNT(*) AS total_nodes,
	nw.last_updated   AS last_updated
FROM   
	nodewatch nw
   	INNER JOIN nodewatch_geo_info ng
   		ON nw.ip = ng.ip
GROUP BY 
	consensus_client,
    hosting_provider_name;

CREATE UNIQUE INDEX bp_key
ON blockprint(proposer_index, slot);

DROP TABLE bp_client;

CREATE TABLE bp_client        AS
WITH client_info
     AS (SELECT proposer_index,
                slot,
                predicted_client
         FROM   blockprint bp1
         WHERE  slot IN (SELECT Max(slot)
                         FROM   blockprint bp2
                         WHERE  bp2.proposer_index = bp1.proposer_index))
SELECT proposer_index   AS validator_index,
       predicted_client AS client
FROM   client_info; 

CREATE UNIQUE INDEX bp_validator_index
ON bp_client(validator_index);

CREATE TABLE ui_validator_first_proposals AS
WITH client_info
     AS (SELECT proposer_index,
                slot,
                predicted_client
         FROM   blockprint bp1
         WHERE  slot IN (SELECT Min(slot)
                         FROM   blockprint bp2
                         WHERE  bp2.proposer_index = bp1.proposer_index)),
     client_info_daily
     AS (SELECT Date_format(From_unixtime(1606824023 + ( 12 * slot )),
                '%Y-%m-%d') AS
                slot_date,
                predicted_client,
                proposer_index,
                Now()
                AS
                   last_updated
         FROM   client_info)
SELECT Date_format(slot_date, '%Y-%m-01') AS first_proposal_month,
       predicted_client,
       Count(DISTINCT proposer_index)     AS total_validators,
       Now()                              AS last_updated
FROM   client_info_daily
WHERE  slot_date >= Date_sub(Date_format(Now(), '%Y-%m-01'), interval 6 month)
GROUP  BY first_proposal_month,
          predicted_client;


DROP TABLE ui_proposals_by_client;

CREATE TABLE ui_proposals_by_client AS
WITH proposal_info
     AS (SELECT Date_format(From_unixtime(1606824023 + ( 12 * slot )),
                '%Y-%m-%d') AS
                slot_date,
                predicted_client,
                Count(*)
                AS
                   total_proposals
         FROM   blockprint
         GROUP  BY slot_date,
                   predicted_client)
SELECT Date_format(slot_date, '%Y-%m-01') AS proposal_month,
       predicted_client,
       SUM(total_proposals)               AS total_proposals,
       Now()                              AS last_updated
FROM   proposal_info
WHERE  slot_date >= Date_sub(Date_format(Now(), '%Y-%m-01'), interval 6 month)
GROUP  BY proposal_month,
          predicted_client; 


CREATE TABLE ui_client_performance AS
  SELECT bc.client                                    AS client,
         bc.validator_index                           AS validator,
         vp.performance31d * 12 / ( 32 * 1000000000 ) AS apr,
         Now()                                        AS last_updated
  FROM   bp_client bc
         INNER JOIN validator_performance vp
                 ON bc.validator_index = vp.validatorindex;

CREATE TABLE ui_depositor_staking AS
  SELECT Coalesce(dl.label, 'Others')   AS depositor_label,
         dl.depositor_type,
         Sum(eth_deposited)             AS total_eth_deposited,
         Sum(eth_deposited_last_30days) AS eth_deposited_last_30days
  FROM   dn_depositor_info di
         LEFT JOIN dn_depositor_labels dl
                ON di.depositor = dl.depositor
  GROUP  BY dl.label;

CREATE TABLE ui_depositor_performance AS
  SELECT dl.label                                     AS depositor_label,
         vp.performance31d * 12 / ( 32 * 1000000000 ) AS apr,
         Now()                                        AS last_updated
  FROM   dn_validators dv
         INNER JOIN dn_depositor_labels dl
                 ON dv.depositor = dl.depositor
         INNER JOIN validator_info v
                 ON dv.validator = v.pubkey
         INNER JOIN validator_performance vp
                 ON v.validatorindex = vp.validatorindex;


CREATE TABLE ui_staking_overview   AS
             with depositor_change AS
             (
                    SELECT sum(
                           CASE
                                  WHEN week >= date_sub(now(), INTERVAL 1 month) THEN depositor_count
                                  ELSE 0
                           end) AS new_depositors,
                           sum(
                           CASE
                                  WHEN week < date_sub(now(), INTERVAL 1 month) THEN depositor_count
                                  ELSE 0
                           end) AS prev_depositors
                    FROM   dn_depositors_signup_weekly
             )
             ,
             validator_change AS
             (
                    SELECT sum(
                           CASE
                                  WHEN first_deposit_week >= date_sub(now(), INTERVAL 1 month) THEN first_deposit_week
                                  ELSE 0
                           end) AS new_validators,
                           sum(
                           CASE
                                  WHEN first_deposit_week < date_sub(now(), INTERVAL 1 month) THEN first_deposit_week
                                  ELSE 0
                           end) AS prev_validators
                    FROM   dn_validators_signup_weekly
             )
      SELECT     num_validators,
                 total_depositors,
                 num_validators * 32             AS eth_deposited,
                 new_depositors /prev_depositors AS depositor_change,
                 new_validators /prev_validators AS validator_change,
                 now()                           AS last_updated
      FROM       dn_total_validators v
      INNER JOIN dn_total_depositors d
      ON         1=1
      INNER JOIN depositor_change dc
      ON         1=1
      INNER JOIN validator_change vc
      ON         1=1;

CREATE TABLE ui_staking_client_distribution AS
SELECT Coalesce(dl.label, 'Unknown') AS staking_entity,
        bc.client,
        Count(*)                      tot_validators,
        Now()                         AS last_updated
FROM   dn_validators dv
        LEFT JOIN dn_depositor_labels dl
            ON dv.depositor = dl.depositor
        INNER JOIN validator_info v
                ON dv.validator = v.pubkey
        INNER JOIN bp_client bc
                ON v.validatorindex = bc.validator_index
GROUP  BY 1,
        2
ORDER  BY 1; 