* 처방과 연결하는 방법
 
1) 영상검사결과
   -- 처방정보와 조인
select *
from 
,( select a.instcd,a.prcpdd,a.pid,a.prcphistcd,a.orddd,a.cretno,a.prcpclscd,b.prcpdd||b.execprcpuniqno as hisorderid,a.fstrgstdt,a.lastupdtdt,a.orddrid,a.prcpnm,a.prcpcd
          from emr.mmohoprc a, emr.mmodexop b
          where a.instcd = b.instcd
          and   a.prcpdd = b.prcpdd
          and   a.prcpno = b.prcpno
          and   a.prcphistno = b.prcphistno
          union all
          select a.instcd,a.prcpdd,a.pid,a.prcphistcd,a.orddd,a.cretno,a.prcpclscd,b.prcpdd||b.execprcpuniqno as hisorderid,a.fstrgstdt,a.lastupdtdt,a.orddrid,a.prcpnm,a.prcpcd
          from emr.mmohiprc a, emr.mmodexip b
          where a.instcd = b.instcd
          and   a.prcpdd = b.prcpdd
          and   a.prcpno = b.prcpno
          and   a.prcphistno = b.prcphistno  ) b
,pac.pacsrpth d
where 1=1 
and   b.pid = d.patid
and   b.hisorderid = d.hisorderid
and   d.queueid = ( select max(queueid)
                         from pac.pacsrpth dd
                         where dd.hisorderid = d.hisorderid)
2) 진단검사결과
  - 처방정보과 조인
  select *
from ( select a.instcd,a.prcpdd,a.pid,a.prcphistcd,a.orddd,a.cretno,a.prcpclscd,b.execprcpuniqno,a.lastupdtdt,a.orddrid,a.prcpnm,a.prcpcd
          from emr.mmohoprc a, emr.mmodexop b
          where a.instcd = b.instcd
          and   a.prcpdd = b.prcpdd
          and   a.prcpno = b.prcpno
          and   a.prcphistno = b.prcphistno
          union all
          select a.instcd,a.prcpdd,a.pid,a.prcphistcd,a.orddd,a.cretno,a.prcpclscd,b.execprcpuniqno ,a.lastupdtdt,a.orddrid,a.prcpnm,a.prcpcd
          from emr.mmohiprc a, emr.mmodexip b
          where a.instcd = b.instcd
          and   a.prcpdd = b.prcpdd
          and   a.prcpno = b.prcpno
          and   a.prcphistno = b.prcphistno  ) b
       , lis.llchsbgd d
       , lis.llrhspdo e
  where 1=1
  and   b.instcd = d.instcd
  and   b.prcpdd = d.prcpdd
  and   b.execprcpuniqno = d.execprcpuniqno
  and   d.instcd = e.instcd
  and   d.bcno   = e.bcno
  and   d.tclscd = e.tclscd
  and   d.spccd  = e.spccd
  and   e.rsltflag = 'O'
   and   e.rsltstat in ('4','5')
   and   b.prcphistcd = 'O'
   and   b.instcd = '031' ;
 
3) 병리검사결과
  - 처방정보와 조인
select     *
from lis.LPRMRSLT b
      ,lis.LPRMCNTS d
      ,lis.LPJMACPT e
      , ( select instcd,pid,orddd,cretno,prcphistcd,prcpdd,prcpno,prcpnm,orddrid,prcpcd
           from emr.mmohiprc
           union all
           select instcd,pid,orddd,cretno,prcphistcd,prcpdd,prcpno,prcpnm,orddrid,prcpcd
           from emr.mmohoprc  ) f
where 1=1
and   b.instcd = d.instcd
and   b.pid = d.pid
and   b.PTNO = d.PTNO
and   b.rsltrgstdd = d.rsltrgstdd
and   b.rsltrgstno = d.rsltrgstno
and   b.rsltrgsthistno = d.rsltrgsthistno
and   e.INSTCD = b.INSTCD
and   e.PTNO   = b.PTNO
and   e.instcd = f.instcd
and   e.prcpdd = f.prcpdd
and   e.prcpno = f.prcpno
and   b.rsltrgsthistno = '1'
and   e.ACPTSTATCD in ('3','4')
and   b.DELFLAGCD ='0'
and   f.prcphistcd = 'O'