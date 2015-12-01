Steps:

1. Match panel data with cert ip data to get group state for all IPs (in order to identify cert vs non cert groups of IPs)

2. Aggregate daily data on all panel variables per IP, per source

3. Aggregate month data per IP, per source


Important Limits:
1. Each IP must have at least 10 users
2. Each IP must have at least 10 messages
3. Each IP must send mail to at least 1 source for at least 5 days

********************
***** Step 1: ******
********************

CREATE TABLE IF NOT EXISTS acase.cert_ips_month
AS
SELECT
    ip_address,
    group_id,
    group_name,
    group_tier,
    ip_state,
    group_state,
    ip_state_sl,
    ip_state_yahoo,
    ip_state_msft,
    day
FROM
     default.rpt_cert_ip
WHERE
    (day < 20151201
AND 
    day >=  20151101); 
    
    
********************
***** Step 2: ******
********************   
 #436,271   
CREATE TABLE IF NOT EXISTS acase.oib_cert_ips_data
AS
SELECT
    t1.ip_address,
    source,
    t1.day,
    group_id,
    group_name,
    group_tier,
    ip_state,
    group_state,
    ip_state_sl,
    ip_state_yahoo,
    ip_state_msft,
    num_domain,
    num_users,
    num_subjects,
    sum_inbox,
    sum_spam,
    sum_trash,
    sum_moved,
    sum_read,
    total_inbox,
    total_msgs,
    inbox_rate,
    read_rate,
    dwoo_rate,
    sum_dwoo
FROM
(
 SELECT
     ip_address,
     source,
     day,
     count(distinct from_domain)  as num_domain,
     count(distinct eea_id)       as num_users,
     count(distinct subject)      as num_subjects,
     SUM(inbox)                   as sum_inbox,
     SUM(spam)                    as sum_spam,
     SUM(trash)                   as sum_trash,
     SUM(moved)                   as sum_moved,
     SUM(email_read)              as sum_read,
     SUM(delete_unopened)         as sum_dwoo,
     SUM(inbox+trash+moved+spam)  as total_msgs,
     SUM(inbox+trash+moved)       as total_inbox,
     SUM(inbox+trash+moved)/
     SUM(inbox+trash+moved+spam)  as inbox_rate,
     SUM(email_read)/
     SUM(inbox+trash+moved+spam)  as read_rate,
     SUM(delete_unopened)/
     SUM(inbox+trash+moved+spam)  as dwoo_rate
 FROM
     default.oib_subscriber
 WHERE
    (day < 20151201
AND 
    day >=  20151101)

 GROUP BY 
     ip_address, 
     source,
     day
) t1
FULL OUTER JOIN
(
     SELECT
         ip_address,
         group_id,
         group_name,
         group_tier,
         ip_state,
         group_state,
         ip_state_yahoo,
         ip_state_msft,
         ip_state_sl,
         day
    FROM
      acase.cert_ips_month
    WHERE 
        (day < 20151201
    AND 
        day >=  20151101)
) t2
ON (t1.ip_address = t2.ip_address
AND t2.day = t1.day)
WHERE num_users >= 10
AND  total_msgs >= 10;


********************
***** Step 3: ******
********************   

CREATE TABLE IF NOT EXISTS acase.oib_cert_ips_data_aggregate
AS
SELECT
    ip_address,
    source,
    group_id,
    group_name,
    group_state,
    count(distinct day)        as num_days,
    SUM(num_domain)    as total_num_domain,
    SUM(num_users)     as total_num_users,
    SUM(num_subjects)  as total_num_subjects,
    SUM(sum_inbox)     as total_sum_inbox,
    SUM(sum_spam)      as total_sum_spam,
    SUM(sum_trash)     as total_sum_trash,
    SUM(sum_moved)     as total_sum_moved,
    SUM(sum_read)      as total_sum_read,
    SUM(total_inbox)   as total_all_inbox,
    SUM(total_msgs)    as total_all_msgs,
    SUM(sum_dwoo)      as total_sum_dwoo,
    AVG(inbox_rate)    as avg_inbox_rate,
    AVG(read_rate)     as avg_read_rate,
    AVG(dwoo_rate)     as avg_dwoo_rate,
    MIN(inbox_rate)    as min_inbox_rate,
    MIN(read_rate)     as min_read_rate,
    MIN(dwoo_rate)     as min_dwoo_rate,
    MAX(inbox_rate)    as max_inbox_rate,
    MAX(read_rate)     as max_read_rate,
    MAX(dwoo_rate)     as max_dwoo_rate,
    STDDEV_POP(num_domain)     as std_num_domains,
    STDDEV_POP(num_users)      as std_num_users,
    STDDEV_POP(num_subjects)   as std_num_subjects,
    STDDEV_POP(sum_inbox)      as std_sum_inbox,
    STDDEV_POP(sum_spam)       as std_sum_spam,
    STDDEV_POP(sum_trash)      as std_sum_trash,
    STDDEV_POP(sum_moved)      as std_sum_moved,
    STDDEV_POP(sum_read)       as std_sum_read,
    STDDEV_POP(total_inbox)    as std_total_inbox,
    STDDEV_POP(total_msgs)     as std_total_msgs,
    STDDEV_POP(inbox_rate)     as std_inbox_rate,
    STDDEV_POP(read_rate)      as std_read_rate,
    STDDEV_POP(dwoo_rate)      as std_dwoo_rate,
    STDDEV_POP(sum_dwoo)       as std_sum_dwoo,
    AVG(num_domain)            as avg_num_domain,
    AVG(num_users)             as avg_num_users,
    AVG(num_subjects)          as avg_num_subjects,
    AVG(sum_inbox)             as avg_sum_inbox,
    AVG(sum_spam)              as avg_sum_spam,
    AVG(sum_trash)             as avg_sum_trash,
    AVG(sum_moved)             as avg_sum_moved,
    AVG(sum_read)              as avg_sum_read,
    AVG(total_inbox)           as avg_total_inbox,
    AVG(total_msgs)            as avg_total_msgs
FROM
    acase.oib_cert_ips_data
GROUP BY
    ip_address,
    source,
    group_id,
    group_name,
    group_state;
            