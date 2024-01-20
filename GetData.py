import numpy as np 
import pandas as pd

import wrds
from tqdm.auto import tqdm

class GetData():
    def __init__(self):
        self.conn = wrds.Connection()
        
    def get_daily_data(self, start_yr, end_yr): 
        for yr in tqdm(range(start_yr, end_yr+1)):
            data_yr = []
            for mth in tqdm(range(1,13)):
                df = self.conn.raw_sql("""select cl.gvkey, d.permno, fund.conm, fund.tic,
                        d."date" ,abs(d.prc) as prc, d.cfacpr, d.vol, d.shrout, d.cfacshr, d.ret, d.bid,  d.ask, d.bidlo, d.askhi
                        
                        from wrds.crsp_q_stock.dsf d  
                        left join  wrds.crsp.ccmxpf_linktable cl 
                            on d.permno  = cl.lpermno 
                            and d."date"  between  cl.linkdt  and coalesce(cl.linkenddt,'9999-12-31')
                        left join lateral 
                            (select cf.filedate ,f.*
                            from wrds.comp.fundq f 
                            left join wrds.comp.co_filedate cf 
                                on f.datadate  = cf.datadate 
                                and f.gvkey  = cf.gvkey 
                            WHERE f.gvkey  = cl.gvkey 
                                and cf.filedate <= d."date" 
                                and cf.srctype  in ('10K','10Q')
                            order by cf.filedate desc 
                            fetch first 1 row only 
                            ) fund
                            on true
                        where d.hexcd  in ('1', '2','3') -- Exchange code for NYSE, NASDAQ and AMEX
                            -- and d."date" >= '2021-01-01' -- Just to check
                            and date_part('year',d."date") = """+str(yr)+"""
                            and date_part('month',d."date") = """+str(mth)+"""
                        
                        
                            and cl.linktype in ('LU' ,'LC') --KEEP reliable LINKS only
                            and cl.linkprim in ('P' ,'C')   --KEEP primary Links
                            and cl.usedflag='1' 
                            and cl.gvkey  is not null 
                        order by d."date", fund.conm """)
                data_yr.append(df)
            pd.concat(data_yr).to_pickle(f"{yr}.pkl")