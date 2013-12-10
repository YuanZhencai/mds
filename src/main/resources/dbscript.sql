-- ddl

create table tab_zhr_mds_001 (begda timestamp not null, endda timestamp not null, pernr varchar(254) not null primary key,nachn varchar(254) not null, name2 varchar(254) not null, gesch varchar(254) not null, icnum varchar(254) not null, usrid_long varchar(254) not null, usrid varchar(254) not null, werks varchar(254) not null, bukrs varchar(254) not null, persg varchar(254) not null, ptext varchar(254) not null, stat2 varchar(254) not null, kostl varchar(254) not null, kostx varchar(254) not null, aedtm timestamp not null, refda timestamp not null);
create table tab_zhr_mds_002 (begda timestamp not null, endda timestamp not null, otype varchar(254) not null, objid varchar(254) not null, short varchar(254) not null, stext varchar(254) not null, zcjid varchar(254) not null, zcjidms varchar(254) not null, zhrzzdwid varchar(254) not null, zhrzzdwms varchar(254) not null, zhrbzzwid varchar(254) not null, zhrbzzwmc varchar(254) not null, zhrzyxid varchar(254) not null, zhrzyxmc varchar(254) not null, zhrbzbmid varchar(254) not null, zhrbzbmmc varchar(254) not null, zhrtxxlid varchar(254) not null, zhrtxxlms varchar(254) not null, zhrjstdid varchar(254) not null, aedtm timestamp not null, refda timestamp not null);
 alter table tab_zhr_mds_002 add constraint pk002 primary key(otype,objid);
create table tab_zhr_mds_003 (begda timestamp not null, endda timestamp not null, otype varchar(254) not null, objid varchar(254) not null, relat varchar(254) not null, sclas varchar(254) not null, sobid varchar(254) not null, prozt varchar(254) not null, priox varchar(254) not null, backup1 varchar(254) not null, backup2 varchar(254) not null, backup3 varchar(254) not null, backup4 varchar(254) not null, aedtm timestamp not null);
 alter table tab_zhr_mds_003 add constraint pk003 primary key(otype,objid,relat,sclas,sobid);
create table sync_log (last_date timestamp not null, processed_at timestamp not null);
 alter table sync_log add constraint pk_sync primary key(last_date,processed_at);

 alter table tab_zhr_mds_001 add column flag1 char(1) not null default 'I';
 alter table tab_zhr_mds_002 add column flag1 char(1) not null default 'I';
 alter table tab_zhr_mds_003 add column flag1 char(1) not null default 'I';

-- indexes

create index i_objid_tab_zhr_mds_002 on tab_zhr_mds_002(objid);
create index i_otype_tab_zhr_mds_002 on tab_zhr_mds_002(otype);
create index i_objid_tab_zhr_mds_003 on tab_zhr_mds_003(objid);
create index i_otype_tab_zhr_mds_003 on tab_zhr_mds_003(otype);
create index i_relat_tab_zhr_mds_003 on tab_zhr_mds_003(relat);
create index i_sclas_tab_zhr_mds_003 on tab_zhr_mds_003(sclas);
create index i_sobid_tab_zhr_mds_003 on tab_zhr_mds_003(sobid);

create index i_last_date_sync_log on sync_log(last_date);

create index i_begda_tab_zhr_mds_001 on tab_zhr_mds_001(begda);
create index i_endda_tab_zhr_mds_001 on tab_zhr_mds_001(endda);
create index i_begda_tab_zhr_mds_002 on tab_zhr_mds_002(begda);
create index i_endda_tab_zhr_mds_002 on tab_zhr_mds_002(endda);
create index i_begda_tab_zhr_mds_003 on tab_zhr_mds_003(begda);
create index i_endda_tab_zhr_mds_003 on tab_zhr_mds_003(endda);

create index i_flag1_tab_zhr_mds_001 on tab_zhr_mds_001(flag1);
create index i_flag1_tab_zhr_mds_002 on tab_zhr_mds_002(flag1);
create index i_flag1_tab_zhr_mds_003 on tab_zhr_mds_003(flag1);


-- views filtering by begda and endda

create or replace view vw_tab_zhr_mds_001(begda, endda, pernr, nachn, name2, gesch, icnum, usrid_long, usrid, werks, bukrs, persg, ptext, stat2, kostl, kostx, aedtm, refda, flag1) as select * from tab_zhr_mds_001 t1 where t1.begda <= current_date and t1.endda >= current_date;
create or replace view vw_tab_zhr_mds_002(begda, endda, otype, objid, short, stext, zcjid, zcjidms, zhrzzdwid, zhrzzdwms, zhrbzzwid, zhrbzzwmc, zhrzyxid, zhrzyxmc, zhrbzbmid, zhrbzbmmc, zhrtxxlid, zhrtxxlms, zhrjstdid, aedtm, refda, flag1) as select * from tab_zhr_mds_002 t2 where t2.begda <= current_date and t2.endda >= current_date;
create or replace view vw_tab_zhr_mds_003(begda, endda, otype, objid, relat, sclas, sobid, prozt, priox, backup1, backup2, backup3, backup4, aedtm, flag1) as select * from tab_zhr_mds_003 t3 where t3.begda <= current_date and t3.endda >= current_date and flag1 <> 'D';

-- vw_wilmar_org

create or replace view vw_manager_by_ou(objid, manager) as
    select distinct on (v1.objid) v1.objid as objid, v2.pernr as manager
    from vw_tab_zhr_mds_002 v1
    left join vw_tab_zhr_mds_003 r1 on (r1.otype = 'O' and r1.objid = v1.objid and r1.relat = '012' and r1.sclas = 'S')
    left join vw_tab_zhr_mds_003 r2 on (r2.otype = 'S' and r2.objid = r1.sobid and r2.relat = '008' and r2.sclas = 'P')
    left join vw_tab_zhr_mds_001 v2 on (v2.pernr = r2.sobid and v2.flag1 <> 'D')
    where v1.otype = 'O'
    order by v1.objid asc, r2.prozt desc;

create or replace view vw_supervisory_by_ou(objid, supervisory) as
    select distinct on (v1.objid) v1.objid as objid, r1.objid as supervisory
    from vw_tab_zhr_mds_002 v1 -- input org
    left join vw_tab_zhr_mds_003 r1 on (r1.otype = 'O' and r1.relat = '002' and r1.sclas = 'O' and r1.sobid = v1.objid)
    where v1.otype = 'O'
    order by v1.objid asc, r1.prozt desc;

create or replace view vw_wilmar_org(ou, diaplay_name, name, wilmar_manager_number, wilmar_supervisory_department, zcjidms, flag1) as
    select 
        v1.objid as ou, 
        v1.short as diaplay_name, 
        v1.stext as name, 
        v2.manager as wilmar_manager_number, 
        v3.supervisory as wilmar_supervisory_department, 
        v1.zcjidms, 
        v1.flag1
    from vw_tab_zhr_mds_002 v1
    left join vw_manager_by_ou v2 on (v2.objid = v1.objid)
    left join vw_supervisory_by_ou v3 on (v3.objid = v1.objid)
    where v1.otype = 'O';

-- vw_org_and_org

create or replace view vw_org_and_org(objid, id, stext, level) as with recursive vw_porg(objid, id, stext, level) as (
    select distinct t0.objid, t0.objid as id ,t0.stext ,'' as level from vw_tab_zhr_mds_002 t0 where t0.otype = 'O' and t0.flag1 <> 'D' 
    union all
    select r1.objid, vw.id ,t1.stext, t1.zcjid
    from vw_tab_zhr_mds_003 r1 , vw_porg vw, vw_tab_zhr_mds_002 t1
    where r1.otype = 'O' and t1.otype = 'O' 
    and r1.sclas = 'O' and r1.relat = '002' and r1.sobid = vw.objid
    and r1.objid = t1.objid and r1.flag1 <> 'D' and t1.flag1 <> 'D' 
)
select * from vw_porg vw where vw.level like '4%' 
group by vw.id, vw.level, vw.objid, vw.stext;

-- vw_wilmar_person

create or replace view vw_wilmar_person(wilmar_join_date, wilmar_leave_date, employee_number, cn, wilmar_english_name, wilmar_gender, mail, telephone_number,
mobile, wilmar_status, employee_type, wilmar_title_code, title, department_number, ou, manager, wilmar_company_code, o, werks, flag1) as
select
    distinct on (v1.pernr)
    v1.begda as wilmar_join_date,
    v1.endda as wilmar_leave_date,
    v1.pernr as employee_number,
    v1.nachn as cn,
    v1.name2 as wilmar_english_name,
    v1.gesch as wilmar_gender,
    case when position('' in v1.usrid_long) > 0 then v1.usrid_long else '' end as mail,
    case when position('' in v1.usrid_long) = 0 then v1.usrid_long else '' end as telephone_number,
    v1.usrid as mobile,
    v1.stat2 as wilmar_status, 
    v1.persg || '-' || v1.ptext as employee_type,
    v2.title as wilmar_title_code,
    v2.title_dscr as title,
    v2.dept as department_number,
    v2.dept_dscr as ou,
    v2.manager as manager,
    v2.company_code as wilmar_company_code,
    v2.company_name as o,
    v1.werks as werks,
    v1.flag1 as flag1
from vw_tab_zhr_mds_001 v1,
(
    select v1.sobid as pernr, v1.prozt as prozt, v2.objid as title, v2.short as title_dscr, v4.objid as dept, v4.short as dept_dscr, v5.manager as manager, v6.objid as company_code, v6.stext as company_name
    from (select * from vw_tab_zhr_mds_003 where otype = 'S' and relat = '008' and sclas = 'P' and flag1 <> 'D') as v1
    inner join vw_tab_zhr_mds_002 v2 on (v2.otype = 'S' and v1.objid = v2.objid and v2.flag1 <> 'D')
    inner join vw_tab_zhr_mds_003 v3 on (v3.otype = 'O' and v3.relat = '003' and v3.sclas = 'S' and v3.sobid = v2.objid and v3.flag1 <> 'D')
    inner join vw_tab_zhr_mds_002 v4 on (v4.otype = 'O' and v4.objid = v3.objid and v4.flag1 <> 'D')
    left join vw_manager_by_ou v5 on (v5.objid = v4.objid)
    left join vw_org_and_org v6 on (v6.id = v4.objid)
) as v2
where v2.pernr = v1.pernr
order by v1.pernr asc, v2.prozt desc;

-- views for tiam

-- views for (org or person) changes by timeline

create or replace view vw_org_changed(objid, at) as
select distinct objid, at from (
    select objid, begda as at from tab_zhr_mds_002 where otype = 'O' and flag1 <> 'D'
    union all
    select objid, endda + interval '1 day' as at from tab_zhr_mds_002 where otype = 'O' and flag1 <> 'D'
    union all
    select objid, aedtm as at from tab_zhr_mds_002 where otype = 'O'
    union all -- manager changed
    select objid, begda as at from tab_zhr_mds_003 where otype = 'O' and relat = '012' and sclas = 'S' and flag1 <> 'D'
    union all -- manager changed
    select objid, endda + interval '1 day' as at from tab_zhr_mds_003 where otype = 'O' and relat = '012' and sclas = 'S' and flag1 <> 'D'
    union all -- manager changed
    select objid, aedtm as at from tab_zhr_mds_003 where otype = 'O' and relat = '012' and sclas = 'S' and flag1 <> 'D'
    union all -- supervisory changed
    select sobid as objid, begda as at from tab_zhr_mds_003 where otype = 'O' and relat = '002' and sclas = 'O' and flag1 <> 'D'
    union all -- supervisory changed
    select sobid as objid, endda + interval '1 day' as at from tab_zhr_mds_003 where otype = 'O' and relat = '002' and sclas = 'O' and flag1 <> 'D'
    union all -- supervisory changed
    select sobid as objid, aedtm as at from tab_zhr_mds_003 where otype = 'O' and relat = '002' and sclas = 'O' and flag1 <> 'D'
) as vw order by at, objid;

create or replace view vw_person_changed(pernr, at) as
select distinct pernr, at from (
    select pernr, begda as at from tab_zhr_mds_001 where flag1 <> 'D'
    union all
    select pernr, endda + interval '1 day' as at from tab_zhr_mds_001 where flag1 <> 'D'
    union all
    select pernr, aedtm as at from tab_zhr_mds_001
    union all
    select sobid as objid, begda as at from tab_zhr_mds_003 where otype = 'S' and relat = '008' and sclas = 'P' and flag1 <> 'D'
    union all
    select sobid as objid, endda + interval '1 day' as at from tab_zhr_mds_003 where otype = 'S' and relat = '008' and sclas = 'P' and flag1 <> 'D'
    union all
    select sobid as objid, aedtm as at from tab_zhr_mds_003 where otype = 'S' and relat = '008' and sclas = 'P' and flag1 <> 'D'
) as vw order by at, pernr;

create or replace view vw_sync_dates as
select distinct processed_at::date as at from sync_log order by at;

create or replace view vw_tiam_org as
select distinct on (v1.at, v2.objid) v1.at, v3.*
from vw_sync_dates as v1
inner join vw_org_changed as v2 on (v2.at >= v1.at and v2.at <= current_timestamp)
inner join vw_wilmar_org as v3 on (v3.ou = v2.objid)
order by v1.at, v2.objid;

create or replace view vw_tiam_per as
select distinct on (v1.at, v2.pernr) v1.at, v3.*
from vw_sync_dates as v1
inner join vw_person_changed as v2 on (v2.at >= v1.at and v2.at <= current_timestamp)
inner join vw_wilmar_person as v3 on (v3.employee_number = v2.pernr)
order by v1.at, v2.pernr;

