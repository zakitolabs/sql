# sql
with raw_trials as (select param_key,param_value
from
     ( VALUES
            ('trial_groups', '{{ trial_groups }}')
            ,('trial_names', '{{ trial_names }}')
            ,('trial_active', '{{ trial_active }}')
    ) as x (param_key, param_value))
 
, trial_groups as (
select trial_group,
row_number() over (partition by param_key) as trial_num,
param_key
from raw_trials
cross join unnest(split(param_value, ';')) as x (trial_group))
 
,trials_list as (
 select param_key, trial_num, trial
 from trial_groups
 cross join unnest(split(trial_group, ',')) as x (trial)
)
 
,trials as (
select a.trial, b.trial as trial_name, c.trial as trial_active
from trials_list a, trials_list b, trials_list c
where a.trial_num = b.trial_num and b.trial_num = c.trial_num
and a.param_key = 'trial_groups' and b.param_key = 'trial_names' and c.param_key = 'trial_active'
)
,raw_skus as (
  select param_key,
    param_value
  from
    ( VALUES
      ('sku_mapping_groups', '{{ sku_mapping_groups }}')
      ,('sku_mapping_names', '{{ sku_mapping_names }}')
    ) as x (param_key, param_value))
 
,sku_groups as (
  select sku_group,
    row_number() over (partition by param_key) as sku_num,
    param_key
  from raw_skus
  cross join unnest(split(param_value, ';')) as x (sku_group))
 
,sku_list as (
  select param_key, sku_num, sku
  from sku_groups
  cross join unnest(split(sku_group, ',')) as x (sku)
  )
   
,skus as (
  select a.sku,
    b.sku as sku_name
  from sku_list a, sku_list b
  where a.sku_num = b.sku_num
    and a.param_key = 'sku_mapping_groups'
    and b.param_key = 'sku_mapping_names'
)
 
, hardware as (
select
  distinct utr.robotid,
  a.key,
  a.value,
  a.sku,
  a.date,
  a.softwarever,
  row_number() over(partition by a.robotid order by a.date DESC) as daterank
from hkc.robot_state_delta a
right join (
select distinct utr.robotid, utr.trial as ota_trial, trial_name, active, date_added, date_removed from hkc_ssd.user_test_robots as utr
join trials
on utr.trial = trials.trial
and cast(utr.active as integer) != cast(trials.trial_active as integer)
where alpha != ({{ alpha }})
) as utr 
on a.robotid = utr.robotid
where a.key ='hwPartsRev'
)
 
, hardware_ver as (
select
 hardware.robotid,
-- value,
-- Hardware versions: https://confluence.irobot.com/pages/viewpage.action?spaceKey=RobotApps&title=Determining+the+Hardware+Version+of+a+Robot
 Case when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '0' THEN 'P2 - No longer supported' 
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '1' THEN 'P4 - No longer supported'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '2' THEN 'P5 - No longer supported'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '3' THEN 'P6'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '4' THEN 'EP1a'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '5' THEN 'EP1b'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '6' THEN 'EP3a'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '7' THEN 'EP3b (with main brush current sense change)'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '8' THEN 'V3 P1'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '9' THEN 'V3 P2 (with 16MHz crystal change)'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '10' THEN 'V3 EP2 (with ST gyro)'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '11' THEN 'V5 G0 version - No longer supported'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '12' THEN 'V3 TMI motor driver'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '13' THEN 'V3 TI + STM32F303 + 144 broadchip'
       when softwarever like '%lewis%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '14' THEN 'V3 TMI + STM32F303 + 144 broadchip'      
        
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '6' THEN 'EP3 - No longer supported'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '7' THEN 'EP3 (with main brush current sensor change) - No longer supported'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '8' THEN 'Lewis V3 P1 configured for Sapphire code - No longer supported'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '9' THEN 'Lewis V3 P2 configured for Sapphire code (with 16 MHz crystal change) - No longer supported'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '10' THEN 'Lewis V3 EP2 configured for Sapphire code (with ST gyro) - No longer supported'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '11' THEN 'Sapphire P2 - No longer supported'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '12' THEN 'Sapphire EP1'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '13' THEN 'Sapphire EP2 (DD mouse)'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '14' THEN '40kHz TI motor driver, no ipropi cap'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '15' THEN '10kHz TI motor driver, ipropi cap'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '16' THEN 'V3 - 10kHz TMI motor driver, no ipropi'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '17' THEN 'V4 - 10kHz TMI motor driver, no ipropi, Awinic lightring'
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '18' THEN ''
       when softwarever like '%sapphire%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '19' THEN ''
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '2' THEN 'not used - No longer supported'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '3' THEN 'not used - No longer supported'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '4' THEN 'EP2 - No longer supported'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '5' THEN 'EP2 (with no bumper micro) - No longer supported'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '6' THEN 'EP3 (with no bumper micro)'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '7' THEN 'EP4 (with over-center design)'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '8' THEN 'EP4 (with lever design)'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '9' THEN 'EP5 (with optical cliffs)'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '10' THEN 'V2 cost down (with 100 pin package and 16MHz crystal change)'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '11' THEN 'V2 PP (EP2)'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '12' THEN 'V2_PP_AWINIC: Awinic LED driver'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '13' THEN 'V2_PP_HALL: Honeywell bumper hall sensor'
       when softwarever like '%sanmarino%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '14' THEN 'V2_PP_AWINIC_HALL: Awinic LED driver and Honeywell bumper hall sensor'
        
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '0' THEN 'P3 - No longer supported'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '1' THEN 'P5 - No longer supported'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '2' THEN 'P5.1 - No longer supported'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '3' THEN 'P6 - No longer supported'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '4' THEN 'P7 - No longer supported'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '5' THEN 'EP1 (with charge pump resets and split clean button)'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '6' THEN 'EP3 (with new hall sensors)'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '7' THEN 'PP (fixes brush motor current gain)'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '8' THEN 'V2'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '9' THEN 'V2 + Honeywell Bumper Board'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '10' THEN 'V2 + Honeywell Bumper Board + Toshiba Blower'
       when softwarever like '%soho%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '11' THEN 'V2 + Honeywell Bumper Board + TI Blower'
        
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '0' THEN 'Lewis V3 EP2 configured for Daredevil code (with ST gyro) - No longer supported'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '1' THEN 'P1 - No longer supported'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '2' THEN 'P2 - No longer supported'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '3' THEN 'EP1'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '4' THEN 'PP and DD-07' 
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '5' THEN 'PP - No longer supported'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '6' THEN 'DD F0 (Rev G) with TMI motor driver'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '7' THEN 'No longer supported'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '8' THEN 'G0-Safety'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '9' THEN 'G0-Power'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '10' THEN 'DD G0 Combo (DD-08)'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '11' THEN 'DD G0 Combo (GEN3C) with TMI motor driver'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '12' THEN 'G0 with 4xTMI motor drivers and LSM6DSR gyro'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '13' THEN 'G0 with TI motor drivers and LSM6DSR gyro'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '14' THEN 'G0 with TI motor drivers and BCT3236 lightring driver'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '15' THEN 'G0 Daredevil with 4xTMI motor drivers and BCT3236 lightring driver'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '16' THEN 'F0, TI motor drivers, BCT3236 lightring driver'
       when softwarever like '%daredevil%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '17' THEN ''          
 
      -- builds for stingray: https://confluence.irobot.com/pages/viewpage.action?pageId=154449946
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '2' THEN 'EP1 Sapphire as Stingray - No longer  supported'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '3' THEN 'P1 - No longer supported'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '4' THEN 'P2'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '5' THEN 'P3'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '6' THEN 'P4'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '7' THEN 'EP1'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '8' THEN 'P4b' -- P4b = P4 + 22N blower motor
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '9' THEN 'EP2'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '10' THEN 'EP3 - Rev A'
    when softwarever like '%stingray%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '11' THEN 'EP3 - Rev B'  
              
    when softwarever like '%magnus%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '0' THEN 'X1'
       when softwarever like '%magnus%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '1' THEN 'X3'
       when softwarever like '%magnus%' AND JSON_EXTRACT_SCALAR(value, '$.mobBrd') = '2' THEN 'X4'
  END as hwVer,
  key,
  lower(regexp_extract(sku, '..')) AS family,
  sku,
  date,
  date_code,
  concat(substr(date_code, 1, 2), '/', substr(date_code, 3, 2)) as manufacture_yymm,
  liferunmin
  from hardware
  left join (select distinct robot_id_nospace, date_code from hkc_cache.iwip_barcode ) as manufacture
  on hardware.robotid = manufacture.robot_id_nospace
  left join
  (select robotid,
  max(cast(json_extract(value, '$.hr') as integer) *60 + cast(json_extract(value, '$.min') as integer)) as liferunmin
  from hkc.robot_state_delta
  where key = 'bbrun'
  group by robotid) as liferun
  on hardware.robotid = liferun.robotid
  where daterank = 1
  ORDER BY robotid
)
 
, versions as (
  SELECT
    mission.robotid,
    mission.nmssn,
    mission.softwarever,
    CASE 
      WHEN regexp_extract(mission.softwarever, '\d+\.\d+\.\d+') is not null                                            
        THEN --assumes no 3 digit values this may need to change in the future
          concat(
          lpad(regexp_extract(mission.softwarever, '(\d+)\.(\d+)\.(\d+)', 1), 2, '0'),'.',
          lpad(regexp_extract(mission.softwarever, '(\d+)\.(\d+)\.(\d+)', 2), 2, '0'),'.',
          lpad(regexp_extract(mission.softwarever, '(\d+)\.(\d+)\.(\d+)', 3), 2, '0'))
      WHEN regexp_extract(mission.softwarever, '\d+\.\d+\.\d+') is null AND regexp_extract(mission.softwarever, '\d{4}\-\d+\-\d{2}') is not NULL
        THEN regexp_extract(mission.softwarever, '\d{4}\-\d+\-\d{2}')
      ELSE mission.softwarever
    END as sw_ver,
    hwVer,
    pmap_id,
    pmapv_id,
    can_be_active,
    create_time,
    creator
  from hkc_cache.mission
  join (
    select
      distinct utr.robotid,
      utr.trial as ota_trial,
      trial_name,
      active,
      date_added,
      date_removed
    from hkc_ssd.user_test_robots as utr
    join trials
      on utr.trial = trials.trial
      and cast(utr.active as integer) != cast(trials.trial_active as integer)
    where alpha != ({{ alpha }})
  ) as utr
    on mission.robotid = utr.robotid
      and mission.timestamp >= date_added
      and mission.timestamp <=
        case
          when date_removed is null
            then 99999999999999
          else date_removed
        end
         
  join hkc_cache.pmapversion as pv
  on pv.robotid = mission.robotid
    and pv.nmssn = mission.nmssn
    and pv.software_ver = mission.softwarever
  left join hardware_ver h on mission.robotid = h.robotid
  WHERE CASE
          WHEN regexp_extract(mission.softwarever, '\d+\.\d+\.\d+') is null
            AND regexp_extract(mission.softwarever, '\d{4}\-\d+\-\d{2}') is not NULL
            then regexp_extract(mission.softwarever, '\d{4}\-\d+\-\d{2}')
          WHEN regexp_extract(mission.softwarever, '\d+\.\d+\.\d+') is not null
            then regexp_extract(mission.softwarever, '\d+\.\d+\.\d+')
          ELSE mission.softwarever
        END not in ({{ versions }})
  and mission.softwarever in (
    SELECT distinct firmware_version
    FROM hkc_ssd.user_test_otas
    )
),
 
maps_data AS (
  SELECT
    DISTINCT robotid,
    m.pmap_id,
    m.pmapv_id,
    m.nmssn,
    sku,
    v.sw_ver,
    hwVer,
    --regexp_extract(v.sw_ver, '\d+\.\d+') as sw_major_minor,
    software_ver as softwarever,
    COALESCE(
      json_extract(data, '$["object"]'),
      json_extract(data, '$["rg-grid"]')
    ) AS data
  FROM
    hkc.v3_map_objects m
    JOIN versions v ON (
      m.pmap_id = v.pmap_id
      AND m.pmapv_id = v.pmapv_id
    )
  WHERE
    header_type = 'type-rendered-grid'
),
maps_cell_data AS (
  SELECT
    *,
    cast(
      json_extract(data, '$["gg-dimensions"]') AS ARRAY(INTEGER)
    ) AS dimensions,
    cast(
      json_extract(data, '$["gg-cells"]') AS ARRAY(INTEGER)
    ) AS cells,
    cardinality(
      cast(
        json_extract(data, '$["gg-cells"]') AS ARRAY(INTEGER)
      )
    ) AS n_cells
  FROM
    maps_data
)
 
,cell_data AS (
  SELECT
    robotid,
    sku,
    pmap_id,
    pmapv_id,
    nmssn,
    softwarever,
    sw_ver,
    hwVer,
  --  sw_major_minor,
    cell_value,
    bitwise_and(cell_value, 7) AS cell_type,
    bitwise_and(cell_value, 128) AS pwf_cell,
    bitwise_and(cell_value, 64) AS edge_cleaned_cell
  FROM
    maps_cell_data
    CROSS JOIN UNNEST(maps_cell_data.cells) AS t (cell_value)
)
, cell_count_data as (
SELECT
  DISTINCT robotid,
  nmssn,
   case
      when sku_name is NULL
        then lower(regexp_extract(m.sku, '..'))
      else sku_name
    end AS family,
  softwarever,
  sw_ver,
  hwVer,
--  sw_major_minor,
  pmap_id,
  pmapv_id,
  count(cell_type) AS n_cells,
  (count(cell_type) - count_if(cell_type = 0)) AS n_occupied,
  --values can be found at: https://confluence.irobot.com/pages/viewpage.action?spaceKey=CLOUD&title=Mission+Map+Data+for+Persistent+Maps
  count_if(cell_type = 0) AS n_unexplored,
  count_if(cell_type = 1) AS n_free,
  count_if(cell_type = 2) AS n_dock,
  count_if(cell_type = 3) AS n_vwall,
  count_if(cell_type = 4) AS n_obstacle,
  count_if(cell_type = 5) AS n_cliff,
  count_if(cell_type = 6) AS n_hazard,
  count_if(cell_type = 7) AS n_invalid,
  count_if(pwf_cell != 0) as n_pwf,
  count_if(edge_cleaned_cell != 0) as n_edge_cleaned
FROM
  cell_data m
left outer join skus
on lower(regexp_extract(m.sku, '..')) = skus.sku
 
WHERE
  nmssn IS NOT NULL
GROUP BY
  robotid,
  pmap_id,
  pmapv_id,
  nmssn,
  softwarever,
  sw_ver,
  hwVer,
 -- sw_major_minor,
  3
ORDER BY
  nmssn DESC
)
 
SELECT distinct c.*, m.pauseid
from cell_count_data c
join hkc_cache.mission m
  on m.robotid = c.robotid and m.nmssn = c.nmssn
right join (
  select
    distinct utr.robotid,
    utr.trial as ota_trial,
    trial_name,
    active,
    date_added,
    date_removed
  from hkc_ssd.user_test_robots as utr
  join trials
    on utr.trial = trials.trial
    and cast(utr.active as integer) != cast(trials.trial_active as integer)
  where alpha != ({{ alpha }})
) as utr
  on c.robotid = utr.robotid
WHERE CASE
          WHEN regexp_extract(c.softwarever, '\d+\.\d+\.\d+') is null
            AND regexp_extract(c.softwarever, '\d{4}\-\d+\-\d{2}') is not NULL
            then regexp_extract(c.softwarever, '\d{4}\-\d+\-\d{2}')
          WHEN regexp_extract(c.softwarever, '\d+\.\d+\.\d+') is not null
            then regexp_extract(c.softwarever, '\d+\.\d+\.\d+')
          ELSE c.softwarever
        END not in ({{ versions }})
  and c.softwarever in (
    SELECT distinct firmware_version
    FROM hkc_ssd.user_test_otas )
