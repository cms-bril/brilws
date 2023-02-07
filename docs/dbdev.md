### Database

online master: cms_omds_lb .p5
    #### read_only account cms_runinfo_r      service name in .ini: online
    #### writer account cms_lumi_w            service name in .ini: onlinew
    #### owner account cms_lumi_prod          not via service

offline adg (oracle data guard): cms_orcon_adg
    #### read_only account cms_runinfo_r      service name in .ini: offline

development: cms_orcoff_prep
    #### owner account                        service name in devdb.ini dev

### Schema: cms_lumi_prod
   #### Table trgscaler column trgprescval NUMBER(10), change it to NUMBER(12,2)
    * work in devdb
    * a set of queries/DDL to alter the table definition keeping the existing data. 
    * a script to select/insert past L1 data to the new table
    

### Interfill data processing scripts
    #### gitlab.cern.ch/bril/utils/load_fromdb.py
        branch devL1float
	branch devDatasetpath
    
    L1trigger: get_trgscaler(r),  load_trgscalertable(w), to_trgscalertable_one(w)
    
    Hlt: get_hltscaler, load_hltscalertable(w), to_hltscalertable_one(w)

================================================================================

### Client software
    #### github.com/xiezhen/brilws  branch
        branch devL1float
	branch devDatasetpath
	
python brilcalc-run.py trg -h	
https://cms-service-lumi.web.cern.ch/cms-service-lumi/brilwsdoc.html

python brilcalc-run.py trg -r 304200 --pathinfo -c dev -p ./devdb.ini


=========== test run
Run-362319 (Fill-8413), LS 40
L1_ZeroBias/2 where L1_ZeroBias/2.7 is expected

L1 prescale source CMS_UGT_MON.VIEW_UGT_RUN_PRESCALE is changed to NUMBER(12,2)

========== back population of data and schema change regression test
need regression test for
     -- new software on new schema
     -- old software on new schema	