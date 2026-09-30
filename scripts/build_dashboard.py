from __future__ import annotations

import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from io import BytesIO, StringIO
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup

SGT = ZoneInfo("Asia/Singapore")
NY_TZ = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
UA = {
    "User-Agent": "MarketDashboard/1.0 contact=personal-dashboard"
}
CACHE_SECONDS = 0

# ---------------- CONFIG ----------------
CROSS_ASSET = {
    'spx': ('^GSPC','S&P 500','Equity'),
    'nasdaq': ('^IXIC','Nasdaq','Equity'),
    'russell': ('^RUT','Russell 2000','Equity'),
    'sox': ('^SOX','SOX','Equity'),
    'es': ('ES=F','S&P 500 Futures','Futures'),
    'nq': ('NQ=F','Nasdaq 100 Futures','Futures'),
    'vix': ('^VIX','VIX','Vol'),
    'stoxx50': ('^STOXX50E','Euro Stoxx 50','Equity'),
    'ftse': ('^FTSE','FTSE 100','Equity'),
    'nikkei': ('^N225','Nikkei 225','Equity'),
    'kospi': ('^KS11','KOSPI','Equity'),
    'hsi': ('^HSI','Hang Seng','Equity'),
    'csi300': ('000300.SS','CSI 300','Equity'),
    'eem': ('EEM','EM Equities ETF','Equity'),
    'dxy': ('DX-Y.NYB','DXY','FX'),
    'usdjpy': ('JPY=X','USD/JPY','FX'),
    'eurusd': ('EURUSD=X','EUR/USD','FX'),
    'usdcnh': ('CNH=X','USD/CNH','FX'),
    'brent': ('BZ=F','Brent','Commodity'),
    'wti': ('CL=F','WTI','Commodity'),
    'gold': ('GC=F','Gold','Commodity'),
    'copper': ('HG=F','Copper','Commodity'),
    'btc': ('BTC-USD','Bitcoin','Crypto'),
}


FACTOR_ETFS = {
    'IWF':'Growth','IWD':'Value','RSP':'Equal Weight','QUAL':'Quality','MTUM':'Momentum','USMV':'Min Volatility'
}

SECTOR_ETFS = {
    'XLK':'Technology','XLC':'Communication','XLI':'Industrials','XLF':'Financials','XLY':'Cons. Discretionary',
    'XLV':'Health Care','XLE':'Energy','XLP':'Cons. Staples','XLU':'Utilities','XLB':'Materials','XLRE':'Real Estate'
}

WATCHLIST = {
    'NVDA':('NVIDIA','Semis','^SOX'),'AMD':('AMD','Semis','^SOX'),'AVGO':('Broadcom','Semis','^SOX'),
    'TSM':('TSMC','Semis','^SOX'),'MU':('Micron','Memory','^SOX'),'ASML':('ASML','Semi Equip','^SOX'),
    'AMAT':('Applied Materials','Semi Equip','^SOX'),'LRCX':('Lam Research','Semi Equip','^SOX'),
    '000660.KS':('SK hynix','Memory','^SOX'),'005930.KS':('Samsung Electronics','Memory','^SOX'),
    'MSFT':('Microsoft','Hyperscaler','^IXIC'),'AMZN':('Amazon','Hyperscaler','^IXIC'),
    'GOOGL':('Alphabet','Hyperscaler','^IXIC'),'META':('Meta','Hyperscaler','^IXIC'),'ORCL':('Oracle','Cloud / AI','^IXIC'),
    'PLTR':('Palantir','AI Software','^IXIC'),'SNOW':('Snowflake','Data / AI','^IXIC'),'NOW':('ServiceNow','SaaS','^IXIC'),
    'CRM':('Salesforce','SaaS','^IXIC'),'ADBE':('Adobe','SaaS','^IXIC'),
    'EQIX':('Equinix','Data Center','^GSPC'),'DLR':('Digital Realty','Data Center','^GSPC'),'CRWV':('CoreWeave','Neocloud','^IXIC'),
    'VRT':('Vertiv','Power / Cooling','^GSPC'),'ETN':('Eaton','Electrical','^GSPC'),'GEV':('GE Vernova','Grid / Power','^GSPC'),
    'CEG':('Constellation Energy','Power','^GSPC'),'NRG':('NRG Energy','Power','^GSPC')
}

TACTICAL_ASSETS = {
    'SPY':('U.S. Equities','Equity','ACWI'),
    'EFA':('DM ex-U.S. Equities','Equity','ACWI'),
    'EEM':('EM Equities','Equity','ACWI'),
    'IWM':('U.S. Small Cap','Equity','SPY'),
    'SHY':('Short U.S. Treasuries','Rates','BIL'),
    'IEF':('Intermediate U.S. Treasuries','Rates','BIL'),
    'TLT':('Long U.S. Treasuries','Rates','BIL'),
    'LQD':('U.S. IG Credit','Credit','IEF'),
    'HYG':('U.S. High Yield','Credit','IEF'),
    'EMB':('EM Hard Currency Debt','Credit','IEF'),
    'GLD':('Gold','Real Asset','BIL'),
    'DBC':('Broad Commodities','Real Asset','BIL'),
    'BIL':('Cash / T-Bills','Cash','BIL'),
}


# Curated cross-sector large-cap universe for broader mover scan.
BROAD_WATCHLIST = {
    'AAPL':('Apple','Technology','XLK'),'QCOM':('Qualcomm','Technology','XLK'),'MRVL':('Marvell','Technology','XLK'),'ANET':('Arista Networks','Technology','XLK'),'DELL':('Dell','Technology','XLK'),'CSCO':('Cisco','Technology','XLK'),
    'JPM':('JPMorgan','Financials','XLF'),'BAC':('Bank of America','Financials','XLF'),'GS':('Goldman Sachs','Financials','XLF'),'MS':('Morgan Stanley','Financials','XLF'),'V':('Visa','Financials','XLF'),'MA':('Mastercard','Financials','XLF'),'FICO':('Fair Isaac','Financials','XLF'),
    'TSLA':('Tesla','Consumer Discretionary','XLY'),'HD':('Home Depot','Consumer Discretionary','XLY'),'LOW':("Lowe's",'Consumer Discretionary','XLY'),'MCD':("McDonald's",'Consumer Discretionary','XLY'),'NKE':('Nike','Consumer Discretionary','XLY'),'BKNG':('Booking Holdings','Consumer Discretionary','XLY'),
    'WMT':('Walmart','Consumer Staples','XLP'),'COST':('Costco','Consumer Staples','XLP'),'PG':('Procter & Gamble','Consumer Staples','XLP'),'KO':('Coca-Cola','Consumer Staples','XLP'),
    'NFLX':('Netflix','Communication','XLC'),'DIS':('Disney','Communication','XLC'),'T':('AT&T','Communication','XLC'),'VZ':('Verizon','Communication','XLC'),
    'LLY':('Eli Lilly','Health Care','XLV'),'UNH':('UnitedHealth','Health Care','XLV'),'JNJ':('Johnson & Johnson','Health Care','XLV'),'ABBV':('AbbVie','Health Care','XLV'),'ISRG':('Intuitive Surgical','Health Care','XLV'),
    'CAT':('Caterpillar','Industrials','XLI'),'BA':('Boeing','Industrials','XLI'),'GE':('GE Aerospace','Industrials','XLI'),'RTX':('RTX','Industrials','XLI'),'HON':('Honeywell','Industrials','XLI'),
    'XOM':('Exxon Mobil','Energy','XLE'),'CVX':('Chevron','Energy','XLE'),'COP':('ConocoPhillips','Energy','XLE'),'SLB':('SLB','Energy','XLE'),
    'FCX':('Freeport-McMoRan','Materials','XLB'),'LIN':('Linde','Materials','XLB'),'PLD':('Prologis','Real Estate','XLRE'),'AMT':('American Tower','Real Estate','XLRE'),'NEE':('NextEra Energy','Utilities','XLU'),'SO':('Southern Company','Utilities','XLU'),
    'UBER':('Uber','Industrials','XLI'),'KMX':('CarMax','Consumer Discretionary','XLY')
}
CORE_GROUP_ORDER=['Semis','Memory','Semi Equip','Hyperscaler','Cloud / AI','AI Software','Data / AI','SaaS','Data Center','Neocloud','Power / Cooling','Electrical','Grid / Power','Power']

FRED_SERIES = {
    'real10': ('DFII10','10Y Real Yield'),
    'ig_oas': ('BAMLC0A0CM','IG OAS'),
    'hy_oas': ('BAMLH0A0HYM2','HY OAS'),
    'ccc_oas': ('BAMLH0A3HYC','CCC OAS'),
}

FOMC_DECISIONS = {
    2026:['2026-10-28','2026-12-09'],
    2027:['2027-01-27','2027-03-17','2027-04-28','2027-06-09','2027-07-28','2027-09-15','2027-10-27','2027-12-08']
}

# ---------------- HELPERS ----------------
def req(url, **kwargs):
    timeout = kwargs.pop('timeout', 10)
    headers = dict(UA); headers.update(kwargs.pop('headers', {}))
    r = requests.get(url, headers=headers, timeout=timeout, **kwargs)
    r.raise_for_status(); return r

def fnum(x):
    try: return float(x)
    except: return None

def clip(x, lo=-1.0, hi=1.0): return max(lo, min(hi, x))
def tanh_score(x, scale=1.0): return 100.0*math.tanh(x*scale)
def sgt_now(): return datetime.now(SGT)

def pct_change(a,b):
    return None if a is None or b in (None,0) else (a/b-1.0)*100.0

# ---------------- YAHOO HISTORY ----------------
def yahoo_history(symbol: str, range_: str='2y'):
    """
    Best-effort public Yahoo chart feed.

    CRITICAL RULE:
    Never use meta.chartPreviousClose for 1D returns. On long-range queries it can
    represent a close before the requested chart range, which caused the earlier
    +100% / +500% false daily-return bug.

    All return horizons are calculated from consecutive adjusted daily bars.
    """
    url=(f'https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol,safe="^=.-")}'
         f'?interval=1d&range={range_}&includePrePost=false&events=div%2Csplits')
    j=req(url, timeout=7).json()['chart']['result'][0]
    ts=j.get('timestamp',[])
    q=j.get('indicators',{}).get('quote',[{}])[0]
    adj=(j.get('indicators',{}).get('adjclose') or [{}])[0].get('adjclose')
    closes=adj or q.get('close',[])
    rows=[]
    for t,c in zip(ts,closes):
        if c is None:
            continue
        rows.append((datetime.fromtimestamp(t,UTC),float(c)))
    if len(rows)<8:
        raise ValueError(f'insufficient history for {symbol}')
    df=(pd.DataFrame(rows,columns=['date','close'])
          .drop_duplicates('date')
          .sort_values('date')
          .reset_index(drop=True))
    return {'df':df,'currency':j.get('meta',{}).get('currency'),'exchange':j.get('meta',{}).get('exchangeName')}

INDEX_SYMBOLS={'^GSPC','^IXIC','^RUT','^SOX','^STOXX50E','^FTSE','^N225','^KS11','^HSI','000300.SS'}
FX_SYMBOLS={'DX-Y.NYB','JPY=X','EURUSD=X','CNH=X'}
COMMODITY_SYMBOLS={'BZ=F','CL=F','GC=F','HG=F'}
FUTURES_SYMBOLS={'ES=F','NQ=F'}
CRYPTO_SYMBOLS={'BTC-USD'}

def _kind_for_symbol(symbol):
    if symbol in INDEX_SYMBOLS: return 'index'
    if symbol in FX_SYMBOLS: return 'fx'
    if symbol in COMMODITY_SYMBOLS: return 'commodity'
    if symbol in FUTURES_SYMBOLS: return 'futures'
    if symbol in CRYPTO_SYMBOLS: return 'crypto'
    if symbol in set(TACTICAL_ASSETS)|set(SECTOR_ETFS)|set(FACTOR_ETFS)|{'ACWI'}: return 'etf'
    return 'stock'

def _sanity_limit(kind):
    # Large enough to allow genuine stress, tight enough to catch data/alignment errors.
    return {'index':20,'fx':10,'commodity':30,'futures':20,'crypto':40,'etf':30,'stock':65}.get(kind,50)

def history_metrics(h, symbol=None):
    df=h['df'].copy()
    closes=df.close
    latest=float(closes.iloc[-1])

    def by_sessions(n):
        if len(df) <= n:
            return None
        return pct_change(latest,float(closes.iloc[-1-n]))

    one_day=by_sessions(1)
    one_week=by_sessions(5)
    one_month=by_sessions(21)
    three_month=by_sessions(63)
    six_month=by_sessions(126)

    latest_date=pd.Timestamp(df.date.iloc[-1]).to_pydatetime()
    jan1=pd.Timestamp(datetime(latest_date.year,1,1,tzinfo=UTC))
    prior_year=df[df.date < jan1]
    if len(prior_year):
        ytd_base=float(prior_year.iloc[-1].close)
    else:
        current_year=df[df.date.dt.year==latest_date.year]
        ytd_base=float(current_year.iloc[0].close) if len(current_year) else None
    ytd=pct_change(latest,ytd_base)

    daily=closes.pct_change().dropna().tail(60)
    vol=float(daily.std()*math.sqrt(252)*100) if len(daily)>=20 else None

    quality_issue=None
    kind=_kind_for_symbol(symbol) if symbol else 'stock'
    limit=_sanity_limit(kind)
    if one_day is not None and abs(one_day)>limit:
        quality_issue=f'1D move {one_day:+.2f}% exceeded {kind} sanity limit {limit}%'
        one_day=None

    return {
        'price':latest,
        '1d':one_day,
        '1w':one_week,
        '1m':one_month,
        '3m':three_month,
        '6m':six_month,
        'ytd':ytd,
        'vol60':vol,
        'asof':latest_date.date().isoformat(),
        'quality_issue':quality_issue,
        'series20':df.tail(20).close.round(4).tolist()
    }


def yahoo_news(ticker, company, max_items=6):
    q=quote(f'{ticker} {company}')
    url=f'https://query1.finance.yahoo.com/v1/finance/search?q={q}&quotesCount=1&newsCount={max_items}&enableFuzzyQuery=false'
    try: items=req(url).json().get('news',[])
    except: return []
    cutoff=datetime.now(UTC)-timedelta(hours=48); out=[]
    for n in items:
        dt=datetime.fromtimestamp(n.get('providerPublishTime'),UTC) if n.get('providerPublishTime') else None
        if dt and dt<cutoff: continue
        out.append({'title':n.get('title','').strip(),'publisher':n.get('publisher',''),'link':n.get('link',''),'dt':dt.isoformat() if dt else None})
    return out

def classify_driver(headline: str):
    h=headline.lower()
    buckets=[
        (['earnings','profit','revenue','guidance','forecast','quarter'],'Earnings / guidance'),
        (['ai','artificial intelligence','gpu','accelerator','data center','datacenter','cloud','capex'],'AI / capex'),
        (['upgrade','downgrade','price target','rating'],'Analyst action'),
        (['china','export','restriction','regulat','antitrust','probe'],'Policy / regulatory'),
        (['debt','bond','financing','credit','offering'],'Financing'),
        (['acquire','acquisition','merger','buyout','stake'],'M&A / transaction'),
        (['order','customer','contract','partnership','deal','supply'],'Order / customer'),
        (['launch','product','chip','platform'],'Product'),
    ]
    for words,label in buckets:
        if any(w in h for w in words): return label
    return 'Company / sector news'

# ---------------- TREASURY / FRED / ACM ----------------
def treasury_curve():
    year=sgt_now().year
    url=f'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value={year}'
    root=ET.fromstring(req(url).text)
    ns={'a':'http://www.w3.org/2005/Atom','m':'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'}
    rows=[]
    for entry in root.findall('a:entry',ns):
        props=entry.find('a:content/m:properties',ns)
        if props is None: continue
        row={c.tag.split('}')[-1]:c.text for c in list(props)}
        if row.get('NEW_DATE'): rows.append(row)
    rows=sorted(rows,key=lambda r:r['NEW_DATE'])
    if len(rows)<22: raise ValueError('insufficient treasury data')
    latest,prev,month=rows[-1],rows[-2],rows[-22]
    fm={'2Y':'BC_2YEAR','5Y':'BC_5YEAR','10Y':'BC_10YEAR','30Y':'BC_30YEAR'}
    out={}
    for k,f in fm.items():
        a,b,m=fnum(latest.get(f)),fnum(prev.get(f)),fnum(month.get(f))
        out[k]={'yield':a,'bp1d':(a-b)*100 if a is not None and b is not None else None,'bp1m':(a-m)*100 if a is not None and m is not None else None,'prev':b,'month':m}
    for name,a,b in [('2s10s','2Y','10Y'),('5s30s','5Y','30Y')]:
        v=(out[b]['yield']-out[a]['yield'])*100
        pv=(out[b]['prev']-out[a]['prev'])*100
        mv=(out[b]['month']-out[a]['month'])*100
        out[name]={'value':v,'bp1d':v-pv,'bp1m':v-mv}
    out['date']=latest['NEW_DATE'][:10]
    return out


def _previous_snapshot():
    try:
        p=Path(__file__).resolve().parents[1]/'site'/'data'/'dashboard.json'
        return json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}
    except Exception:
        return {}

def _series_stats(df, date_col='DATE', value_col='VALUE'):
    x=df[[date_col,value_col]].copy()
    x[date_col]=pd.to_datetime(x[date_col],errors='coerce')
    x[value_col]=pd.to_numeric(x[value_col],errors='coerce')
    x=x.dropna().sort_values(date_col)
    if len(x)<2:
        raise ValueError('insufficient history')
    a,b=x.iloc[-1],x.iloc[-2]
    month=x.iloc[-22] if len(x)>=22 else x.iloc[0]
    return {
        'value':float(a[value_col]),
        '1d':float(a[value_col]-b[value_col]),
        '1m':float(a[value_col]-month[value_col]),
        'date':a[date_col].strftime('%Y-%m-%d')
    }

def treasury_real10():
    """Official U.S. Treasury real curve; avoids FRED dependency for the 10Y real yield."""
    year=sgt_now().year
    url=('https://home.treasury.gov/resource-center/data-chart-center/interest-rates/'
         f'TextView?type=daily_treasury_real_yield_curve&field_tdr_date_value={year}')
    html=req(url,timeout=18).text
    tables=pd.read_html(StringIO(html))
    for t in tables:
        cols={str(x).strip().upper():x for x in t.columns}
        dcol=next((orig for key,orig in cols.items() if key=='DATE'),None)
        vcol=next((orig for key,orig in cols.items() if key.replace(' ','') in ('10YR','10YEAR')),None)
        if dcol is not None and vcol is not None:
            x=t[[dcol,vcol]].rename(columns={dcol:'DATE',vcol:'VALUE'})
            out=_series_stats(x)
            out['label']='10Y Real Yield'
            out['source']='U.S. Treasury'
            return out
    raise ValueError('Treasury real-yield table unavailable')

def _fred_html_history(series_id):
    """FRED table page fallback: no API key and usually more reliable than fredgraph.csv on CI runners."""
    url=f'https://fred.stlouisfed.org/data/{series_id}'
    html=req(url,timeout=14).text
    tables=pd.read_html(StringIO(html))
    for t in tables:
        cols={str(x).strip().upper():x for x in t.columns}
        if 'DATE' in cols and 'VALUE' in cols:
            x=t[[cols['DATE'],cols['VALUE']]].rename(columns={cols['DATE']:'DATE',cols['VALUE']:'VALUE'})
            return _series_stats(x)
    # FRED sometimes renders the data as plain text instead of a semantic table.
    text=BeautifulSoup(html,'html.parser').get_text('\n',strip=True)
    pairs=re.findall(r'(20\d{2}-\d{2}-\d{2})\s*[| ]+\s*(-?\d+(?:\.\d+)?)',text)
    if len(pairs)>=2:
        return _series_stats(pd.DataFrame(pairs,columns=['DATE','VALUE']))
    raise ValueError(f'FRED data page parse unavailable for {series_id}')

def _fred_csv_history(series_id):
    start=(sgt_now().date()-timedelta(days=550)).isoformat()
    end=sgt_now().date().isoformat()
    url=f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start}&coed={end}'
    txt=req(url,timeout=10).text
    df=pd.read_csv(StringIO(txt))
    val=df.columns[-1]
    return _series_stats(df.rename(columns={val:'VALUE'}))

def fred_history(series_id):
    errors=[]
    for fn in (_fred_html_history,_fred_csv_history):
        try:
            return fn(series_id)
        except Exception as exc:
            errors.append(str(exc))
    raise ValueError(' | '.join(errors))

def _cached_or_seed(key,label,seed):
    prev=_previous_snapshot().get('fred',{}).get(key,{})
    if isinstance(prev,dict) and prev.get('value') is not None:
        out={k:prev.get(k) for k in ('value','1d','1m','date')}
        out.update({'label':label,'stale':True,'source_status':'last-good cache'})
        return out
    out=dict(seed)
    out.update({'label':label,'stale':True,'source_status':'seed fallback'})
    return out

def fred_pack():
    """Public-site macro pack. ICE OAS is intentionally not redistributed; credit uses liquid ETF proxies instead."""
    try:
        return {'real10':treasury_real10()}
    except Exception:
        prev=_previous_snapshot().get('fred',{}).get('real10',{})
        if isinstance(prev,dict) and prev.get('value') is not None:
            out={k:prev.get(k) for k in ('value','1d','1m','date')}
            out.update({'label':'10Y Real Yield','stale':True,'source_status':'last-good cache'})
            return {'real10':out}
        return {'real10':{'value':2.85,'1d':0.09,'1m':0.47,'date':'2026-09-24','label':'10Y Real Yield','stale':True,'source_status':'seed fallback'}}

def acm_term_premium():
    raw=req('https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls',timeout=25).content
    xls=pd.ExcelFile(BytesIO(raw)); sheet='ACM Daily' if 'ACM Daily' in xls.sheet_names else xls.sheet_names[0]
    df=pd.read_excel(BytesIO(raw),sheet_name=sheet)
    cols=[c for c in ['DATE','ACMY10','ACMTP10','ACMRNY10'] if c in df.columns]
    df=df[cols]; df['DATE']=pd.to_datetime(df['DATE'],errors='coerce')
    for c in cols[1:]: df[c]=pd.to_numeric(df[c],errors='coerce')
    df=df.dropna(subset=['DATE','ACMTP10']).sort_values('DATE')
    a,b,m=df.iloc[-1],df.iloc[-2],df.iloc[-22] if len(df)>=22 else df.iloc[0]
    return {'tp10':float(a.ACMTP10),'bp1d':float((a.ACMTP10-b.ACMTP10)*100),'bp1m':float((a.ACMTP10-m.ACMTP10)*100),'date':a.DATE.strftime('%Y-%m-%d'),
            'fitted10':fnum(a.get('ACMY10')),'risk_neutral10':fnum(a.get('ACMRNY10'))}


def cme_cvol():
    """Best-effort CME web value with last-good continuity; never blanks the dashboard on a 403."""
    urls=[
      'https://www.cmegroup.com/markets/interest-rates.html',
      'https://www.cmegroup.com/cn-s/markets/interest-rates.html'
    ]
    headers={
      'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/153 Safari/537.36',
      'Accept-Language':'en-US,en;q=0.9'
    }
    for url in urls:
        try:
            text=BeautifulSoup(req(url,timeout=10,headers=headers).text,'html.parser').get_text(' ',strip=True)
            m=re.search(r'Treasury (?:Yield )?CVOL Index.*?Code:\s*TVL.*?Cvol:\s*([0-9.]+).*?(?:Change|涨跌):\s*([+\-0-9.]+).*?(?:Last Updated|最后更新)\s*([^<]{0,60})',text,re.I)
            if not m:
                m=re.search(r'Treasury (?:Yield )?CVOL Index.*?Code:\s*TVL.*?Cvol:\s*([0-9.]+).*?(?:Change|涨跌):\s*([+\-0-9.]+)',text,re.I)
            if m:
                return {'value':float(m.group(1)),'change':float(m.group(2)),'date':m.group(3).strip() if len(m.groups())>=3 and m.group(3) else None,'stale':False,'source':'CME public page'}
        except Exception:
            pass
    prev=_previous_snapshot().get('cvol',{})
    if isinstance(prev,dict) and prev.get('value') is not None:
        out={k:prev.get(k) for k in ('value','change','date')}
        out.update({'stale':True,'source_status':'last-good cache'})
        return out
    return {'value':130.7204,'change':7.6710,'date':'2026-09-28','stale':True,'source_status':'CME public-page seed'}

def fedwatch():
    try:
        from cme_fedwatch import get_probabilities
        d=get_probabilities('next'); mtg=(d.get('meetings') or [{}])[0]
        probs=mtg.get('probabilities',{})
        return {'available':True,'date':mtg.get('date'),'probabilities':probs,'trade_date':d.get('trade_date')}
    except Exception as e:
        return {'available':False,'error':str(e)}

# ---------------- CALENDAR ----------------
def parse_ics(text):
    lines=[]
    for raw in text.replace('\r\n','\n').split('\n'):
        if raw.startswith(' ') and lines: lines[-1]+=raw[1:]
        else: lines.append(raw)
    events=[]; cur=None
    for ln in lines:
        if ln=='BEGIN:VEVENT': cur={}
        elif ln=='END:VEVENT' and cur is not None: events.append(cur); cur=None
        elif cur is not None and ':' in ln:
            k,v=ln.split(':',1); cur[k]=v
    return events


CALENDAR_GROUPS = {
    'PCE Inflation': {
        'source':'BEA','importance':'HIGH',
        'detail_url':'https://www.bea.gov/data/personal-consumption-expenditures-price-index'
    },
    'GDP': {
        'source':'BEA','importance':'HIGH',
        'detail_url':'https://www.bea.gov/data/gdp/gross-domestic-product'
    },
    'CPI Inflation': {
        'source':'BLS','importance':'HIGH',
        'detail_url':'https://www.bls.gov/cpi/'
    },
    'PPI Inflation': {
        'source':'BLS','importance':'HIGH',
        'detail_url':'https://www.bls.gov/ppi/'
    },
    'Jobs Report': {
        'source':'BLS','importance':'HIGH',
        'detail_url':'https://www.bls.gov/news.release/empsit.toc.htm'
    },
    'JOLTS': {
        'source':'BLS','importance':'HIGH',
        'detail_url':'https://www.bls.gov/jlt/'
    },
    'ISM Manufacturing': {
        'source':'ISM','importance':'HIGH',
        'detail_url':'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/'
    },
    'ISM Services': {
        'source':'ISM','importance':'HIGH',
        'detail_url':'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/'
    },
    'Retail Sales': {
        'source':'Census','importance':'HIGH',
        'detail_url':'https://www.census.gov/retail/index.html'
    },
    'ADP Employment': {
        'source':'ADP','importance':'MEDIUM',
        'detail_url':'https://adpemploymentreport.com/'
    },
    'FOMC Decision': {
        'source':'Federal Reserve','importance':'HIGH',
        'detail_url':'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    },
}

CALENDAR_METRICS = [
    (r'^Core PCE Price Index MoM\b','PCE Inflation','Core PCE MoM',1),
    (r'^Core PCE Price Index YoY\b','PCE Inflation','Core PCE YoY',2),
    (r'^PCE Price Index MoM\b','PCE Inflation','Headline PCE MoM',3),
    (r'^PCE Price Index YoY\b','PCE Inflation','Headline PCE YoY',4),

    (r'^GDP Growth Rate QoQ','GDP','Real GDP QoQ SAAR',1),
    (r'^GDP Price Index QoQ','GDP','GDP Price Index',2),

    (r'^Non Farm Payrolls\b','Jobs Report','Nonfarm Payrolls',1),
    (r'^Unemployment Rate\b','Jobs Report','Unemployment Rate',2),
    (r'^Average Hourly Earnings MoM\b','Jobs Report','Average Hourly Earnings MoM',3),
    (r'^Average Hourly Earnings YoY\b','Jobs Report','Average Hourly Earnings YoY',4),

    (r'^JOLTs Job Openings\b','JOLTS','Job Openings',1),

    (r'^ISM Manufacturing PMI\b','ISM Manufacturing','Headline PMI',1),
    (r'^ISM Manufacturing New Orders\b','ISM Manufacturing','New Orders',2),
    (r'^ISM Manufacturing Prices\b','ISM Manufacturing','Prices Paid',3),
    (r'^ISM Manufacturing Employment\b','ISM Manufacturing','Employment',4),

    (r'^ISM Services PMI\b','ISM Services','Headline PMI',1),
    (r'^ISM Services New Orders\b','ISM Services','New Orders',2),
    (r'^ISM Services Prices\b','ISM Services','Prices Paid',3),
    (r'^ISM Services Employment\b','ISM Services','Employment',4),

    (r'^Core Inflation Rate MoM\b','CPI Inflation','Core CPI MoM',1),
    (r'^Core Inflation Rate YoY\b','CPI Inflation','Core CPI YoY',2),
    (r'^Inflation Rate MoM\b','CPI Inflation','Headline CPI MoM',3),
    (r'^Inflation Rate YoY\b','CPI Inflation','Headline CPI YoY',4),

    (r'^Core PPI MoM\b','PPI Inflation','Core PPI MoM',1),
    (r'^Core PPI YoY\b','PPI Inflation','Core PPI YoY',2),
    (r'^PPI MoM\b','PPI Inflation','Headline PPI MoM',3),
    (r'^PPI YoY\b','PPI Inflation','Headline PPI YoY',4),

    (r'^Retail Sales MoM\b','Retail Sales','Headline Retail Sales MoM',1),
    (r'^Retail Sales Ex Autos MoM\b','Retail Sales','Ex-Autos MoM',2),
    (r'^Retail Sales Control Group MoM\b','Retail Sales','Control Group MoM',3),

    (r'^ADP Employment Change(?! Weekly)\b','ADP Employment','ADP Employment Change',1),
    (r'^(?:Fed Interest Rate Decision|Federal Funds Rate)\b','FOMC Decision','Fed Funds Target',1),
]

def _calendar_metric_spec(name):
    clean=re.sub(r'\s+',' ',str(name or '')).strip()
    for pat,group,metric,priority in CALENDAR_METRICS:
        if re.search(pat,clean,re.I):
            return group,metric,priority
    return None

def _clean_calendar_value(x):
    x=re.sub(r'\s+',' ',str(x or '')).strip()
    x=x.replace('®','').replace('Â','').strip()
    return '' if x in {'','-','—','nan','None'} else x

def _calendar_surprise(actual,consensus):
    a=_clean_calendar_value(actual); b=_clean_calendar_value(consensus)
    if not a or not b: return ''
    def parse(v):
        m=re.search(r'([-+]?\d+(?:\.\d+)?)\s*(%|K|M|B|T)?',v,re.I)
        return (float(m.group(1)),(m.group(2) or '')) if m else (None,'')
    av,au=parse(a); bv,bu=parse(b)
    if av is None or bv is None or au.upper()!=bu.upper(): return ''
    diff=av-bv; unit=au.upper()
    if unit=='%':
        return f'{diff:+.1f}pp'
    if unit in {'K','M','B','T'}:
        return f'{diff:+.1f}{unit}'
    return f'{diff:+.1f}'

def _te_calendar_rows():
    """Selected U.S. macro events from the public Trading Economics calendar page."""
    now=sgt_now()
    d1=(now.date()-timedelta(days=2)).isoformat()
    d2=(now.date()+timedelta(days=45)).isoformat()
    urls=[
        f'https://tradingeconomics.com/united-states/calendar?from={d1}&to={d2}',
        f'https://tradingeconomics.com/calendar?from={d1}&to={d2}&countries=United%20States'
    ]
    headers={
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/153 Safari/537.36',
        'Accept-Language':'en-US,en;q=0.9'
    }
    last_error=None
    for url in urls:
        try:
            html=req(url,timeout=18,headers=headers).text
            soup=BeautifulSoup(html,'html.parser')
            current_date=None; rows=[]
            for tr in soup.find_all('tr'):
                txt=re.sub(r'\s+',' ',tr.get_text(' ',strip=True))
                dm=re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+([A-Z][a-z]+)\s+(\d{1,2})\s+(\d{4})',txt)
                if dm:
                    try:
                        current_date=datetime.strptime(f'{dm.group(2)} {dm.group(3)} {dm.group(4)}','%B %d %Y').date()
                    except: pass
                    if len(tr.find_all('td'))<4:
                        continue

                cells=tr.find_all('td')
                if len(cells)<5: continue

                # Find the event cell by matching one of our selected macro event names.
                event_name=''; event_idx=None; event_href=''
                for idx,td in enumerate(cells):
                    celltxt=_clean_calendar_value(td.get_text(' ',strip=True))
                    if _calendar_metric_spec(celltxt):
                        event_name=celltxt; event_idx=idx
                        a=td.find('a',href=True)
                        if a:
                            href=a.get('href','')
                            event_href=href if href.startswith('http') else ('https://tradingeconomics.com'+href if href.startswith('/') else '')
                        break
                if event_idx is None: continue

                # Recover row date from attributes when a date header was not encountered.
                row_date=current_date
                if row_date is None:
                    attrs=' '.join(str(v) for v in tr.attrs.values())
                    mm=re.search(r'(20\d{2}-\d{2}-\d{2})',attrs)
                    if mm:
                        try: row_date=datetime.fromisoformat(mm.group(1)).date()
                        except: pass
                if row_date is None: continue

                time_txt=''
                for td in cells[:event_idx]:
                    t=_clean_calendar_value(td.get_text(' ',strip=True))
                    if re.fullmatch(r'\d{1,2}:\d{2}\s*(?:AM|PM)',t,re.I):
                        time_txt=t; break
                if not time_txt: continue

                try:
                    tm=datetime.strptime(time_txt.upper(),'%I:%M %p').time()
                    dt_utc=datetime.combine(row_date,tm).replace(tzinfo=UTC)
                    dt_sgt=dt_utc.astimezone(SGT)
                except:
                    continue

                spec=_calendar_metric_spec(event_name)
                if not spec: continue
                group,metric,priority=spec

                vals=[_clean_calendar_value(td.get_text(' ',strip=True)) for td in cells[event_idx+1:event_idx+5]]
                while len(vals)<4: vals.append('')
                actual,previous,consensus,forecast=vals[:4]

                period=''
                pm=re.search(r'\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|Q[1-4])(?:/\d+)?\b',event_name,re.I)
                if pm: period=pm.group(0).upper()

                rows.append({
                    'dt':dt_sgt,'group':group,'metric':metric,'priority':priority,
                    'actual':actual,'previous':previous,'consensus':consensus,'forecast':forecast,
                    'surprise':_calendar_surprise(actual,consensus),
                    'period':period,'market_url':event_href or 'https://tradingeconomics.com/united-states/calendar',
                    'raw_event':event_name
                })
            if rows:
                return rows
            last_error=ValueError('no selected macro events parsed')
        except Exception as exc:
            last_error=exc
    raise ValueError(f'Trading Economics calendar unavailable: {last_error}')

def _official_calendar_fallback():
    """Release-date fallback from official calendars. Values remain blank rather than invented."""
    rows=[]; now=datetime.now(NY_TZ)
    # BLS calendar
    try:
        for e in parse_ics(req('https://www.bls.gov/schedule/news_release/bls.ics',timeout=12).text):
            summary=e.get('SUMMARY',''); lo=summary.lower(); group=None
            if 'employment situation' in lo: group='Jobs Report'
            elif 'consumer price index' in lo: group='CPI Inflation'
            elif 'producer price index' in lo: group='PPI Inflation'
            elif 'job openings and labor turnover' in lo and 'state ' not in lo: group='JOLTS'
            if not group: continue
            dtline=next((v for k,v in e.items() if k.startswith('DTSTART')),None)
            if not dtline: continue
            try: dt=datetime.strptime(dtline[:15],'%Y%m%dT%H%M%S').replace(tzinfo=NY_TZ) if 'T' in dtline else datetime.strptime(dtline[:8],'%Y%m%d').replace(hour=8,minute=30,tzinfo=NY_TZ)
            except: continue
            if dt>=now-timedelta(days=2):
                rows.append((dt.astimezone(SGT),group))
    except: pass

    # BEA release schedule
    try:
        for df in pd.read_html(StringIO(req('https://www.bea.gov/news/schedule/',timeout=12).text)):
            for _,row in df.astype(str).iterrows():
                txt=' | '.join(row.tolist()); lo=txt.lower(); group=None
                if 'personal income and outlays' in lo: group='PCE Inflation'
                elif 'gdp' in lo and ('estimate' in lo or 'gross domestic product' in lo): group='GDP'
                if not group: continue
                m=re.search(r'([A-Z][a-z]+)\s+(\d{1,2})\s+(\d{1,2}:\d{2})\s*(AM|PM)',txt)
                if not m: continue
                try: dt=datetime.strptime(f'{m.group(1)} {m.group(2)} {now.year} {m.group(3)} {m.group(4)}','%B %d %Y %I:%M %p').replace(tzinfo=NY_TZ)
                except: continue
                if dt>=now-timedelta(days=2): rows.append((dt.astimezone(SGT),group))
    except: pass

    # ISM published cadence, official rule: first and third business days at 10am ET.
    for offset in range(3):
        month=(now.month-1+offset)%12+1; year=now.year+(now.month-1+offset)//12
        weekdays=[]; x=date(year,month,1)
        while x.month==month and len(weekdays)<5:
            if x.weekday()<5: weekdays.append(x)
            x+=timedelta(days=1)
        manu=weekdays[0]; serv=weekdays[2]
        for dd,group in [(manu,'ISM Manufacturing'),(serv,'ISM Services')]:
            dt=datetime(dd.year,dd.month,dd.day,10,0,tzinfo=NY_TZ)
            if dt>=now-timedelta(days=2): rows.append((dt.astimezone(SGT),group))

    # FOMC decisions
    for yr,dates in FOMC_DECISIONS.items():
        for ds in dates:
            dt=datetime.fromisoformat(ds).replace(hour=14,minute=0,tzinfo=NY_TZ)
            if dt>=now-timedelta(days=2): rows.append((dt.astimezone(SGT),'FOMC Decision'))

    out=[]
    seen=set()
    for dt,group in sorted(rows,key=lambda x:x[0]):
        key=(dt.date(),group)
        if key in seen: continue
        seen.add(key)
        meta=CALENDAR_GROUPS[group]
        out.append({
            'iso_date':dt.date().isoformat(),'date':dt.strftime('%a, %d %b'),'time':dt.strftime('%H:%M'),
            'title':group,'period':'','importance':meta['importance'],'source':meta['source'],
            'detail_url':meta['detail_url'],'market_url':'https://tradingeconomics.com/united-states/calendar',
            'status':'RELEASED' if dt<=sgt_now() else 'UPCOMING','cached':False,
            'metrics':[]
        })
    return out

def major_calendar():
    now=sgt_now()
    try:
        raw=_te_calendar_rows()
    except Exception:
        raw=[]

    groups={}
    for r in raw:
        dt=r['dt']
        if dt < now-timedelta(hours=36): continue
        if dt > now+timedelta(days=45): continue
        key=(dt.strftime('%Y-%m-%d'),r['group'])
        g=groups.setdefault(key,{
            'dt':dt,'title':r['group'],'period':r.get('period',''),
            'metrics':[],'market_url':r.get('market_url') or 'https://tradingeconomics.com/united-states/calendar'
        })
        if not g.get('period') and r.get('period'): g['period']=r['period']
        if dt < g['dt']: g['dt']=dt
        g['metrics'].append({
            'name':r['metric'],'previous':r['previous'],'consensus':r['consensus'],
            'actual':r['actual'],'forecast':r['forecast'],'surprise':r['surprise'],
            'priority':r['priority']
        })

    out=[]
    for g in groups.values():
        meta=CALENDAR_GROUPS[g['title']]
        metrics=sorted(g['metrics'],key=lambda x:x['priority'])
        for m in metrics: m.pop('priority',None)
        dt=g['dt']
        out.append({
            'iso_date':dt.date().isoformat(),'date':dt.strftime('%a, %d %b'),'time':dt.strftime('%H:%M'),
            'title':g['title'],'period':g.get('period',''),'importance':meta['importance'],'source':meta['source'],
            'detail_url':meta['detail_url'],'market_url':g['market_url'],
            'status':'RELEASED' if dt<=now else 'UPCOMING','cached':False,'metrics':metrics
        })

    # If public calendar parsing is temporarily unavailable, carry last-good values,
    # then merge official release dates so the section never disappears.
    prev=_previous_snapshot().get('calendar',[])
    if not out and isinstance(prev,list):
        for e in prev:
            try:
                dd=datetime.fromisoformat(e.get('iso_date','')).replace(tzinfo=SGT)
                if dd.date() >= (now.date()-timedelta(days=1)):
                    z=dict(e); z['cached']=True; out.append(z)
            except: pass

    fallback=_official_calendar_fallback()
    existing={(e.get('iso_date'),e.get('title')) for e in out}
    for e in fallback:
        k=(e.get('iso_date'),e.get('title'))
        if k not in existing:
            out.append(e); existing.add(k)

    # Keep recent releases plus enough future horizon to always include CPI/PPI/PCE/jobs/retail/FOMC.
    out=sorted(out,key=lambda e:(e.get('iso_date',''),e.get('time',''),0 if e.get('status')=='RELEASED' else 1,e.get('title','')))
    recent=[e for e in out if e.get('status')=='RELEASED'][-3:]
    upcoming=[e for e in out if e.get('status')!='RELEASED'][:14]
    return recent+upcoming

# ---------------- SIGNAL ENGINE ----------------
def standardized_momentum(m, horizon_key, vol):
    r=m.get(horizon_key)
    if r is None or vol in (None,0): return 0.0
    days={'1m':21,'3m':63,'6m':126}[horizon_key]
    horizon_vol=vol*math.sqrt(days/252.0)
    return clip((r/100.0)/(horizon_vol/100.0+1e-6),-2,2)

def build_risk_regime(histories, market, fred):
    # returns score in [-1,1]
    def mom(sym,key='1m'):
        h=histories.get(sym); return (h or {}).get(key) or 0.0
    sp=clip(mom('SPY')/6.0)
    credit=clip((mom('HYG')-mom('LQD'))/4.0)
    vix=(market.get('vix') or {}).get('price')
    vix_score=0.0 if vix is None else clip((22.0-vix)/10.0)
    real1m=(fred.get('real10') or {}).get('1m')
    real_score=0.0 if real1m is None else clip(-(real1m*100)/25.0)
    risk=0.35*sp+0.25*credit+0.20*vix_score+0.20*real_score
    # growth proxy: small caps, cyclicals, copper/gold
    growth=0.4*clip((mom('IWM')-mom('SPY'))/4.0)+0.3*clip((mom('XLI')-mom('XLU'))/4.0)+0.3*clip(((market.get('copper') or {}).get('1m') or 0)-((market.get('gold') or {}).get('1m') or 0),-10,10)/10
    # inflation proxy: oil + breakeven direction
    br=(market.get('brent') or {}).get('1m') or 0
    inf=0.65*clip(br/12.0)+0.35*(0 if real1m is None else 0)  # second leg overwritten below if breakeven history unavailable
    return {'risk':clip(risk),'growth':clip(growth),'inflation':clip(inf)}

def tactical_signals(histories, regime):
    out=[]
    risk=regime['risk']; inflation=regime['inflation']
    for sym,(name,cls_,bench) in TACTICAL_ASSETS.items():
        m=histories.get(sym)
        if not m: continue
        trend_raw=0.50*standardized_momentum(m,'1m',m.get('vol60'))+0.30*standardized_momentum(m,'3m',m.get('vol60'))+0.20*standardized_momentum(m,'6m',m.get('vol60'))
        trend=clip(trend_raw/1.3)
        if bench==sym or bench not in histories: rel=0.0
        else:
            r=(m.get('3m') or 0)-(histories[bench].get('3m') or 0)
            rel=clip(r/8.0)
        if cls_=='Equity': macro=risk
        elif cls_=='Credit': macro=(0.85*risk - 0.15*inflation)
        elif cls_=='Rates': macro=(-0.55*risk - 0.45*inflation)
        elif cls_=='Real Asset': macro=(-0.25*risk + 0.75*inflation) if sym=='GLD' else (0.35*risk+0.65*inflation)
        else: macro=-0.65*risk
        score=100*(0.50*trend+0.25*rel+0.25*macro)
        score=max(-100,min(100,score))
        view='OVERWEIGHT' if score>=25 else 'UNDERWEIGHT' if score<=-25 else 'NEUTRAL'
        comps={'Trend':50*trend,'Relative':25*rel,'Macro':25*macro}
        top=max(comps,key=lambda k:abs(comps[k]))
        driver=f'{top} {"supportive" if comps[top]>=0 else "negative"}'
        out.append({'symbol':sym,'name':name,'class':cls_,'view':view,'score':round(score,1),'1d':m.get('1d'),'1m':m.get('1m'),'3m':m.get('3m'),'6m':m.get('6m'),'vol':m.get('vol60'),'driver':driver,
                    'trend':round(100*trend,1),'relative':round(100*rel,1),'macro':round(100*macro,1)})
    return out

# ---------------- INTERPRETATION ----------------
def curve_label(c):
    a=(c.get('2Y') or {}).get('bp1d'); b=(c.get('10Y') or {}).get('bp1d')
    if a is None or b is None: return 'Curve unavailable'
    steep=(c.get('2s10s') or {}).get('bp1d') or 0
    if a>0 and b>0: base='Bear steepening' if steep>1 else 'Bear flattening' if steep<-1 else 'Parallel selloff'
    elif a<0 and b<0: base='Bull steepening' if steep>1 else 'Bull flattening' if steep<-1 else 'Parallel rally'
    elif a<0<b: base='Twist steepening'
    elif a>0>b: base='Twist flattening'
    else: base='Mixed curve'
    return base

def rates_interpretation(c,fred,acm):
    y2=(c.get('2Y') or {}).get('bp1d'); y10=(c.get('10Y') or {}).get('bp1d'); y30=(c.get('30Y') or {}).get('bp1d')
    s=(c.get('2s10s') or {}).get('bp1d'); parts=[]
    if None not in (y2,y10,y30,s): parts.append(f'{curve_label(c)}: 2Y {y2:+.0f}bp / 10Y {y10:+.0f}bp / 30Y {y30:+.0f}bp; 2s10s {s:+.0f}bp.')
    r=(fred.get('real10') or {}).get('1d'); tp=(acm or {}).get('bp1d')
    if r is not None: parts.append(f'10Y real yield {r*100:+.0f}bp.')
    if tp is not None: parts.append(f'ACM term premium {tp:+.0f}bp on latest model date.')
    return ' '.join(parts)

def top_takeaways(payload):
    m=payload['market']; c=payload['curve']; movers=payload['movers']; sig=payload['signals']; out=[]
    # 1 equity leadership
    sp=(m.get('spx') or {}).get('1d'); sox=(m.get('sox') or {}).get('1d'); nd=(m.get('nasdaq') or {}).get('1d')
    if None not in (sp,sox,nd):
        if abs(sox-nd)>=1: out.append(f'Semis {sox:+.2f}% vs Nasdaq {nd:+.2f}% — semiconductor-specific leadership/divergence is material.')
        else: out.append(f'S&P {sp:+.2f}% / Nasdaq {nd:+.2f}% / SOX {sox:+.2f}% — equity move is relatively broad.')
    # 2 rates
    out.append(rates_interpretation(c,payload['fred'],payload['acm']))
    # 3 credit/risk — same-day liquid ETF proxy for the public dashboard
    cp=payload.get('credit_proxy',{}); hyig=(cp.get('hy_vs_ig') or {}).get('1d')
    if hyig is not None:
        out.append(f'Credit proxy: HYG vs LQD {hyig:+.2f}% 1D — ' + ('HY is underperforming IG, a same-day risk warning.' if hyig<0 else 'HY is outperforming IG, so credit beta is not confirming stress.'))
    # 4 mover — only show a stock-specific takeaway when a catalyst is actually supported.
    if movers and movers[0].get('explanation'):
        out.append(f'{movers[0]["display"]} {movers[0]["move"]:+.2f}% — {movers[0]["explanation"]}')
    # 5 next catalyst — more useful in the morning header than repeating tactical allocation.
    cal=payload.get('calendar',[])
    nxt=next((e for e in cal if e.get('status')!='RELEASED'),None)
    if nxt:
        detail=''
        mets=nxt.get('metrics') or []
        if mets:
            m0=mets[0]
            if m0.get('consensus'):
                detail=f" · {m0.get('name')}: cons. {m0.get('consensus')}"
            elif m0.get('forecast'):
                detail=f" · {m0.get('name')}: model f/c {m0.get('forecast')}"
        out.append(f"Next catalyst: {nxt.get('title')} · {nxt.get('date')} {nxt.get('time')} SGT{detail}.")
    return out[:4]


def build_commentary(payload):
    """Build a detailed daily desk commentary that explains the whole day, not just the numbers."""
    m=payload.get('market',{}); c=payload.get('curve',{}); f=payload.get('fred',{})
    acm=payload.get('acm',{}); be=payload.get('breakeven',{}); cp=payload.get('credit_proxy',{})
    sectors=payload.get('sectors',[]); factors=payload.get('factors',[])
    core=payload.get('core_tape',[]); broad=payload.get('broad_movers',[])
    signals=payload.get('signals',[]); cal=payload.get('calendar',[])
    regime=payload.get('regime',{}); cv=payload.get('cvol',{})

    def mv(k,field='1d'): return (m.get(k) or {}).get(field)
    def level(k): return (m.get(k) or {}).get('price')
    def bps(x): return '—' if x is None else f'{x:+.0f} bp'
    def pct(x): return '—' if x is None else f'{x:+.2f}%'
    def num(x,d=2): return '—' if x is None else f'{x:,.{d}f}'
    def get_factor(name): return next((x for x in factors if x.get('name')==name),None)
    def get_core(t): return next((x for x in core if x.get('display')==t),None)

    spx,ndx,sox,rut=mv('spx'),mv('nasdaq'),mv('sox'),mv('russell')
    vix=level('vix')
    y2=(c.get('2Y') or {}).get('bp1d'); y10=(c.get('10Y') or {}).get('bp1d'); y30=(c.get('30Y') or {}).get('bp1d')
    y2lvl=(c.get('2Y') or {}).get('yield'); y10lvl=(c.get('10Y') or {}).get('yield'); y30lvl=(c.get('30Y') or {}).get('yield')
    steep=(c.get('2s10s') or {}).get('bp1d')
    real=(f.get('real10') or {}).get('1d'); reallvl=(f.get('real10') or {}).get('value')
    tp=acm.get('bp1d'); tplvl=acm.get('tp10')
    be1=be.get('bp1d'); belvl=be.get('value')
    ig=(f.get('ig_oas') or {}).get('1d'); hy=(f.get('hy_oas') or {}).get('1d'); ccc=(f.get('ccc_oas') or {}).get('1d')
    iglvl=(f.get('ig_oas') or {}).get('value'); hylvl=(f.get('hy_oas') or {}).get('value'); ccclvl=(f.get('ccc_oas') or {}).get('value')
    brent,gold,copper,dxy=mv('brent'),mv('gold'),mv('copper'),mv('dxy')
    usdjpy,usdcnh,btc=mv('usdjpy'),mv('usdcnh'),mv('btc')
    risk=round((regime.get('risk') or 0)*100); growth=round((regime.get('growth') or 0)*100); inf=round((regime.get('inflation') or 0)*100)

    if None not in (sox,ndx,y2,y10) and sox>ndx+1 and y2<0<y10:
        headline='AI leadership held up while the Treasury curve sent a different message'
        dek=('The day was not a clean risk-on or risk-off session. AI / semiconductor leadership stayed firm, '
             'but the front end of the Treasury curve eased while the long end sold off. The main macro question is therefore '
             'less “is the Fed more hawkish?” and more “why is long-duration risk still being repriced?”')
    elif risk<=-25:
        headline='Cross-asset confirmation turned defensive'
        dek=('The tape is increasingly consistent with risk aversion: equities are softer, credit is more fragile and '
             'macro-sensitive markets are losing support. The key issue is whether this remains a rates-led correction or '
             'develops into a broader growth / funding event.')
    elif risk>=30:
        headline='Risk appetite remains constructive, but the quality of the rally matters'
        dek=('Headline equity performance is supportive, but a durable risk-on signal requires breadth, credit and cyclical '
             'markets to confirm. The dashboard therefore focuses less on the S&P alone and more on who is leading underneath it.')
    else:
        headline='Mixed tape: the market is sending more than one message'
        dek=('The useful read comes from sequencing the evidence. Start with equity leadership, then the Treasury curve, '
             'then credit, and only after that use FX and commodities to decide whether the dominant impulse is growth, inflation or risk aversion.')

    evidence=[]
    if None not in (spx,ndx,sox):
        evidence.append({'label':'Equity leadership','metric':f'SOX {pct(sox)} vs Nasdaq {pct(ndx)}',
                         'tone':'positive' if sox>ndx+0.75 else 'negative' if sox<ndx-0.75 else 'neutral',
                         'text':'Semis are leading independently of broad tech, so the headline index is understating AI-specific strength.' if sox>ndx+0.75 else
                                'Semis are lagging broad tech, making the semiconductor complex the weak link.' if sox<ndx-0.75 else
                                'Semis and broad tech are moving together, so there is no major sector divergence.'})
    if None not in (y2,y10,y30):
        evidence.append({'label':'Rates signal','metric':f'2Y {bps(y2)} · 10Y {bps(y10)} · 30Y {bps(y30)}',
                         'tone':'negative' if y10>0 or y30>0 else 'positive',
                         'text':'Front end down / long end up is a twist steepener and points toward long-duration pressure, not a pure Fed-path selloff.' if y2<0<y10 else
                                'The front end is doing more of the work, so policy-path repricing is the first explanation to test.' if y2>y10+3 else
                                'The move is fairly broad across maturities, so there is no single maturity-specific signal.'})
    if None not in (ig,hy,ccc):
        evidence.append({'label':'Credit confirmation','metric':f'IG {bps(ig*100)} · HY {bps(hy*100)} · CCC {bps(ccc*100)}',
                         'tone':'negative' if ccc>0 and hy>0 else 'positive' if ccc<=0 and hy<=0 else 'neutral',
                         'text':'Widening increases down the quality stack — the pattern to watch when a rates shock is becoming a real funding / default-risk event.' if ccc>hy>ig else
                                'Credit is not confirming broad stress, so the move is more likely rates / positioning-led.' if hy<=0 and ccc<=0 else
                                'Credit confirmation is mixed; the equity and rates story has not fully spread into funding markets.'})
    evidence.append({'label':'Regime composite','metric':f'Risk {risk:+d} · Growth {growth:+d} · Inflation {inf:+d}',
                     'tone':'negative' if risk<-25 else 'positive' if risk>25 else 'neutral',
                     'text':'These scores summarize cross-asset inputs; the change in the score matters more than the label itself.'})

    growth_factor=get_factor('Growth'); value_factor=get_factor('Value'); equal=get_factor('Equal Weight')
    factor_text=''
    if growth_factor and value_factor:
        factor_text += f'Growth moved {pct(growth_factor.get("1d"))} versus Value {pct(value_factor.get("1d"))}. '
    if equal and mv('spx','1m') is not None and equal.get('1m') is not None:
        gap=equal.get('1m')-mv('spx','1m')
        factor_text += f'Equal Weight is {gap:+.2f}% versus the S&P over one month, ' + ('which says index gains are relatively narrow.' if gap<-1 else 'which says breadth is broadly keeping pace.')

    eq_body=''
    if None not in (spx,ndx,sox):
        eq_body=(f'The S&P moved {pct(spx)}, Nasdaq {pct(ndx)} and SOX {pct(sox)}. VIX was {num(vix,1) if vix is not None else "—"}. ')
        if sox>ndx+1:
            eq_body+=('The important point is not simply that “tech was strong”; semiconductor leadership was much stronger than broad technology. '
                      'That is more consistent with a concentrated AI / compute trade than a generic falling-discount-rate rally. ')
        elif sox<ndx-1:
            eq_body+=('The semiconductor complex lagged broad technology, so the weakness looks more sector-specific than a generic duration problem. ')
        if rut is not None and spx is not None:
            eq_body+=f'Russell 2000 moved {pct(rut)}. ' + ('Small-cap outperformance improves the breadth signal and is more consistent with a cyclical tape. ' if rut>spx+0.5 else 'Small caps did not materially outperform, so the headline index move still lacks strong breadth confirmation. ')
        eq_body+=factor_text
    else:
        eq_body='Equity data are incomplete, so the breadth read is lower confidence.'

    eq_read=('Read this section as “who is taking risk?” rather than simply “did the index go up?” If SOX, small caps, cyclicals and Equal Weight all participate, '
             'the move is broad and healthier. If only mega-cap growth / semis rise while Equal Weight and small caps lag, the market is narrower and more dependent on a few themes.')
    eq_why=('Equity breadth tells you whether portfolio risk should be interpreted as a broad macro trade or a concentrated thematic trade. '
            'Concentrated leadership can coexist with weak credit or high real yields for a while, but it is usually more fragile.')

    rates_body=f'The Treasury curve was {curve_label(c).lower()}. '
    if None not in (y2,y10,y30):
        rates_body+=f'2Y was {num(y2lvl,2)}% ({bps(y2)}), 10Y {num(y10lvl,2)}% ({bps(y10)}) and 30Y {num(y30lvl,2)}% ({bps(y30)}). '
        if steep is not None: rates_body+=f'2s10s changed {bps(steep)}. '
    if y2 is not None and y10 is not None:
        if y2<0<y10:
            rates_body+=('This is the key distinction for the day: the Fed-sensitive front end rallied while the long end sold off. '
                         'If the whole move were simply “Fed more hawkish,” the 2Y would normally be the first place to reprice higher. Instead, the market is demanding more compensation further out the curve. ')
        elif y2>y10+3:
            rates_body+=('The front end moved more than the long end, which makes policy expectations the first explanation to test. In that setup, FedWatch, inflation data and labor data matter more than Treasury supply explanations. ')
    decomp=[]
    if reallvl is not None: decomp.append(f'10Y real yield {num(reallvl,2)}%' + (f' ({real*100:+.0f} bp)' if real is not None else ''))
    if belvl is not None: decomp.append(f'10Y breakeven proxy {num(belvl,2)}%' + (f' ({be1:+.0f} bp)' if be1 is not None else ''))
    if tplvl is not None: decomp.append(f'ACM term premium {num(tplvl,2)}%' + (f' ({tp:+.0f} bp)' if tp is not None else ''))
    if decomp: rates_body+='The long-rate decomposition was: '+', '.join(decomp)+'. '
    if cv.get('value') is not None:
        rates_body+=f'Treasury CVOL was {cv.get("value"):.1f}' + (f' ({cv.get("change"):+.1f})' if cv.get('change') is not None else '') + ', which measures the amount of rate volatility being priced, not the direction of yields. '

    rates_read=('The practical sequence is: 2Y tells you about the near-term Fed path; 10Y real yield tells you about the real discount rate; breakeven tells you about inflation compensation; '
                'ACM term premium tells you about the extra compensation investors require to hold long duration. A 10Y selloff led by term premium is a very different market message from one led by the 2Y.')
    rates_why=('Rates are the discount-rate engine for the rest of the dashboard. Real yields matter most for long-duration equities, REITs and gold; term premium matters for long bonds and valuation multiples; '
               'the 2Y matters for Fed-sensitive assets. The same +10 bp move in the 10Y can therefore have very different implications depending on the decomposition.')

    credit_body=''
    if None not in (iglvl,hylvl,ccclvl):
        credit_body=f'Latest spread levels were IG {iglvl*100:,.0f} bp, HY {hylvl*100:,.0f} bp and CCC {ccclvl*100:,.0f} bp. '
    if None not in (ig,hy,ccc):
        credit_body+=f'Latest changes were IG {ig*100:+.0f} bp, HY {hy*100:+.0f} bp and CCC {ccc*100:+.0f} bp. '
        if ccc>hy>ig:
            credit_body+=('The widening gets larger as quality falls. That is more important than a small equity decline because it says the market is demanding more compensation specifically for weaker balance sheets and default risk. ')
        elif hy<=0 and ccc<=0:
            credit_body+=('Credit is stable-to-tighter, so it is not confirming a broad risk event. That makes a rates / valuation / positioning explanation more likely. ')
        else:
            credit_body+=('The credit signal is mixed. Avoid calling the day a full risk-off event until lower-quality spreads confirm. ')
    credit_read=('Use credit as the confirmation test. Equities can move 1–2% on rates, positioning or earnings without implying a deterioration in corporate fundamentals. '
                 'When HY and especially CCC widen sharply at the same time, the probability that the move reflects funding stress, weaker growth or default risk rises.')
    credit_why=('CCC matters even if you do not own CCC bonds because it is the part of the market most sensitive to refinancing and cash-flow stress. '
                'A widening sequence of IG < HY < CCC is a stronger macro warning than a uniform move across quality buckets.')

    macro_body=''
    if dxy is not None:
        macro_body+=f'DXY moved {pct(dxy)}'
        if usdjpy is not None: macro_body+=f', USD/JPY {pct(usdjpy)}'
        if usdcnh is not None: macro_body+=f' and USD/CNH {pct(usdcnh)}'
        macro_body+='. '
    if None not in (brent,gold,copper):
        macro_body+=f'Brent moved {pct(brent)}, gold {pct(gold)} and copper {pct(copper)}. '
        if brent>1: macro_body+=('Firm oil keeps the inflation tail alive. If long yields are also rising, that is especially uncomfortable for long-duration assets because it raises both the nominal discount rate and inflation uncertainty. ')
        if copper>gold+1: macro_body+='Copper materially outperforming gold is a more cyclical / growth-positive signal. '
        elif gold>copper+1: macro_body+='Gold materially outperforming copper is more defensive and hedge-heavy. '
    if btc is not None: macro_body+=f'Bitcoin moved {pct(btc)}, useful as a secondary liquidity / risk-appetite check rather than a primary macro signal. '
    macro_read=('FX and commodities are not the starting point; they are cross-checks. A stronger dollar plus higher U.S. real yields usually tightens financial conditions. '
                'Oil is both a growth and inflation input, so the context matters. Copper versus gold is a simple way to ask whether commodity leadership is cyclical or defensive.')
    macro_why=('These markets help distinguish similar-looking equity moves. Stocks down + oil up + yields up is different from stocks down + oil down + gold up. '
               'The first looks more inflation / rates driven; the second is more consistent with growth fear or risk aversion.')

    stock_body=''
    if sectors:
        top=sectors[0]; bottom=sectors[-1]
        stock_body+=f'Sector leadership was {top["name"]} {top["1d"]:+.2f}% versus {bottom["name"]} {bottom["1d"]:+.2f}%. '
        if top.get('rel1m') is not None: stock_body+=f'{top["name"]} is {top["rel1m"]:+.2f}% versus the S&P over one month. '
    key_core=sorted([x for x in core if x.get('material')],key=lambda z:abs(z.get('move') or 0),reverse=True)[:5]
    if key_core:
        stock_body+='Within the core coverage list, the most important moves were '+', '.join(f'{x["display"]} {x["move"]:+.2f}%' for x in key_core)+'. '
        for x in key_core[:3]:
            if x.get('explanation') and x.get('explanation')!='—': stock_body+=f'{x["display"]}: {x["explanation"]} '
    if broad:
        stock_body+='Outside the core AI / infrastructure universe, the broader large-cap scan highlighted '+', '.join(f'{x["display"]} {x["move"]:+.2f}%' for x in broad[:3])+'. '
        for x in broad[:2]:
            if x.get('explanation'): stock_body+=f'{x["display"]}: {x["explanation"]} '
    stock_read=('The important distinction is sector move versus idiosyncratic move. Compare every stock with its benchmark. A +3% move when the sector is +2.5% is less company-specific than a +3% move when the sector is flat. '
                'That is why the dashboard shows the stock’s move and its relative move together.')
    stock_why=('Single-name dispersion often tells you where the market’s real narrative is. Hyperscalers tell you about AI capex and cloud economics; semis tell you about compute / memory demand; '
               'VRT / ETN / GEV / CEG tell you about the power bottleneck; broader movers catch non-AI shocks that can still move the index or signal sector stress.')

    ow=sorted([x for x in signals if x['view']=='OVERWEIGHT'],key=lambda z:-z['score'])
    uw=sorted([x for x in signals if x['view']=='UNDERWEIGHT'],key=lambda z:z['score'])
    neutral=[x for x in signals if x['view']=='NEUTRAL']
    alloc_body=''
    if ow: alloc_body+='Highest tactical scores: '+', '.join(f'{x["name"]} {x["score"]:+.0f}' for x in ow[:4])+'. '
    if uw: alloc_body+='Lowest tactical scores: '+', '.join(f'{x["name"]} {x["score"]:+.0f}' for x in uw[:4])+'. '
    if neutral: alloc_body+=f'{len(neutral)} assets remain Neutral. '
    alloc_body+=('The signal uses 50% multi-horizon trend, 25% relative strength and 25% macro / regime alignment, so it changes gradually rather than flipping on one headline.')
    alloc_read=('Treat OW / Neutral / UW as a disciplined summary of current market evidence, not a forecast and not a portfolio mandate. '
                'The score answers “where is the current tape most supportive?” rather than “what should the portfolio own regardless of valuation, liquidity or strategic constraints?”')
    alloc_why=('This prevents the daily commentary from becoming purely narrative. If the written story says equities are constructive but the systematic score is deteriorating, that disagreement itself is information and should lower conviction.')

    next_body=''
    if cal:
        next_body+='The next major catalysts are '+'; '.join(f'{e["title"]} on {e["date"]} at {e["time"]} SGT' for e in cal[:4])+'. '
    if y2 is not None and y10 is not None and y2<0<y10:
        next_body+=('The cleanest test of today’s long-end thesis is the next macro release: if data are soft, 2Y stays down and 10Y / 30Y remain high, the market is confirming a term-premium / supply problem. '
                    'If 2Y jumps as well, the interpretation shifts back toward Fed repricing. ')
    if hy is not None and ccc is not None:
        next_body+=('A second test is credit: if HY and especially CCC continue to widen while equities weaken, the move is broadening from rates stress into a genuine risk event. ')
    next_read=('Before each event, decide which asset should react first. Inflation / labor data should hit the 2Y first. Treasury auctions should show up most clearly in the long end and term premium. '
               'Growth data should show up in cyclicals, small caps, copper and credit. This reaction map makes the next move interpretable instead of surprising.')
    next_why=('The goal of the morning dashboard is not only to explain yesterday. It should tell you what evidence would confirm or invalidate the current story today.')

    deep_sections=[
        {'num':'01','title':'Risk tone & breadth','headline':'Was the move broad or concentrated?','body':eq_body,'read':eq_read,'why':eq_why},
        {'num':'02','title':'Rates & Fed','headline':'Which part of the curve actually repriced?','body':rates_body,'read':rates_read,'why':rates_why},
        {'num':'03','title':'Credit','headline':'Did funding markets confirm the risk move?','body':credit_body,'read':credit_read,'why':credit_why},
        {'num':'04','title':'FX & commodities','headline':'Growth, inflation or risk aversion?','body':macro_body,'read':macro_read,'why':macro_why},
        {'num':'05','title':'Sectors & stocks','headline':'Where was the real dispersion?','body':stock_body,'read':stock_read,'why':stock_why},
        {'num':'06','title':'Tactical allocation','headline':'What is the systematic tape saying?','body':alloc_body,'read':alloc_read,'why':alloc_why},
        {'num':'07','title':'Next catalysts','headline':'What would confirm or break today’s story?','body':next_body,'read':next_read,'why':next_why},
    ]

    stock_notes=[]
    if sectors: stock_notes.append(f'Sector leader: {sectors[0]["name"]} {sectors[0]["1d"]:+.2f}% · laggard: {sectors[-1]["name"]} {sectors[-1]["1d"]:+.2f}%.')
    for t in ['META','AMZN','NVDA','MU']:
        r=get_core(t)
        if r:
            stock_notes.append(f'{t} {r["move"]:+.2f}% · {r["relative"]:+.2f}% vs benchmark' + (f' · {r["explanation"]}' if r.get('material') and r.get('explanation') else ''))
    if broad: stock_notes.append(f'Broader-market mover: {broad[0]["display"]} {broad[0]["move"]:+.2f}% · {broad[0]["sector"]}.')

    positioning={'ow':[{'name':x['name'],'score':x['score']} for x in ow[:3]],
                 'uw':[{'name':x['name'],'score':x['score']} for x in uw[:3]],
                 'text':'Tactical 1–6M signal. Use it as a disciplined summary of trend / relative strength / macro alignment, not as a strategic portfolio recommendation.'}

    checks=[]
    if y2 is not None and y10 is not None and y2<0<y10:
        checks.append({'event':'Long-end thesis test','test':'Soft data + 2Y lower + 10Y/30Y still high = term premium / supply confirmed. 2Y higher too = story shifts back toward Fed repricing.'})
    for ev in cal[:3]:
        checks.append({'event':f'{ev["title"]} · {ev["date"]} {ev["time"]} SGT','test':'Ask which asset should react first, then check whether the rest of the cross-asset tape confirms.'})
    if hy is not None and ccc is not None:
        checks.append({'event':'Credit confirmation','test':'Further HY / CCC widening alongside weaker equities would confirm that the move is becoming a broader risk event.'})
    checks=checks[:5]

    meeting=[]
    if None not in (spx,ndx,sox):
        meeting.append(f'U.S. equities were mixed: S&P {pct(spx)}, Nasdaq {pct(ndx)} and SOX {pct(sox)}.')
        if sox>ndx+1: meeting.append('The key equity message was concentrated semiconductor / AI leadership rather than a broad growth rally.')
    if None not in (y2,y10,y30):
        meeting.append(f'Rates were more important: 2Y {bps(y2)}, 10Y {bps(y10)} and 30Y {bps(y30)}.')
        if y2<0<y10: meeting.append('Because the front end rallied while the long end sold off, the move looks more like long-duration / term-premium pressure than a simple hawkish-Fed repricing.')
    hyig=(cp.get('hy_vs_ig') or {}).get('1d'); cp_date=(cp.get('hy') or {}).get('asof') or (cp.get('ig') or {}).get('asof')
    if hyig is not None:
        if hyig<-0.20:
            meeting.append(f'Credit confirmed some stress: HYG underperformed LQD by {abs(hyig):.2f}% on {cp_date}.')
        elif hyig>0.20:
            meeting.append(f'Credit did not confirm broad stress: HYG outperformed LQD by {hyig:.2f}% on {cp_date}.')
        else:
            meeting.append(f'Credit was broadly neutral: HYG versus LQD was {hyig:+.2f}% on {cp_date}.')
    if brent is not None:
        if brent>1:
            meeting.append(f'Brent rose {pct(brent)}, keeping the inflation tail relevant for long-duration assets.')
        elif brent<-1:
            meeting.append(f'Brent fell {pct(brent)}, which offsets part of the inflation pressure and adds a softer growth / demand signal.')
        else:
            meeting.append(f'Brent was little changed at {pct(brent)}, so oil did not materially change the macro read.')
    if core:
        biggest=max(core,key=lambda x:abs(x.get('move') or 0))
        meeting.append(f'The largest core-stock move was {biggest["display"]} {biggest["move"]:+.2f}%.')
    if cal: meeting.append(f'The next major catalyst is {cal[0]["title"]} at {cal[0]["time"]} SGT; watch the 2Y first, then the long end and credit for confirmation.')
    meeting_summary=' '.join(meeting)

    bottom=('The day should be read as a chain, not as separate boxes: first identify equity leadership and breadth; second decide whether rates are being driven by the Fed path, real yields, inflation compensation or term premium; '
            'third ask whether credit confirms the risk move; fourth use the dollar and commodities to classify the macro impulse; fifth check whether the single-name tape agrees with the sector story; and finally map the next catalyst to the asset that should react first. '
            'If those pieces line up, conviction is high. If they conflict, keep the interpretation conditional rather than forcing one narrative.')

    # Connected daily storyline: one main spine, with explicit branches where the tape diverges.
    if None not in (y2,y10) and y2<0<y10:
        story_start={
          'kicker':'STARTING POINT · RATES',
          'title':'The front end eased, but the long end sold off',
          'body':f'2Y moved {bps(y2)} while 10Y moved {bps(y10)} and 30Y {bps(y30)}. That immediately argues against a simple “Fed turned more hawkish” explanation and shifts the first question to long-duration compensation: real yields, inflation compensation and term premium.',
          'tone':'warning'
        }
    elif brent is not None and abs(brent)>=2 and y10 is not None and y10>0:
        story_start={
          'kicker':'STARTING POINT · INFLATION',
          'title':'The first pressure point was inflation-sensitive markets',
          'body':f'Brent moved {pct(brent)} while the 10Y rose {bps(y10)}. That combination raises the nominal discount rate and keeps the inflation tail alive, so long-duration assets need stronger earnings or theme support to offset the macro drag.',
          'tone':'warning'
        }
    elif None not in (sox,ndx) and abs(sox-ndx)>=1:
        story_start={
          'kicker':'STARTING POINT · EQUITY LEADERSHIP',
          'title':'The index hid a much bigger leadership split underneath',
          'body':f'SOX moved {pct(sox)} versus Nasdaq {pct(ndx)}. The first useful read was therefore not “stocks up or down,” but whether the market was rewarding a specific AI / semiconductor theme or a broad growth impulse.',
          'tone':'supportive' if sox>ndx else 'warning'
        }
    else:
        story_start={
          'kicker':'STARTING POINT · CROSS-ASSET',
          'title':'The day began as a mixed cross-asset signal',
          'body':f'S&P {pct(spx)}, Nasdaq {pct(ndx)}, SOX {pct(sox)} and 10Y {bps(y10)} did not line up into one clean risk-on / risk-off message. The right starting point is therefore to trace which markets confirmed each other and which did not.',
          'tone':'mixed'
        }

    transmission_bits=[]
    if None not in (y2,y10,y30):
        if y2<0<y10:
            transmission_bits.append('Rates transmitted through the long end rather than the Fed-sensitive front end.')
        elif y2>0 and y10>0:
            transmission_bits.append('Rates repriced broadly higher, so both policy expectations and duration pressure mattered.')
        elif y2<0 and y10<0:
            transmission_bits.append('Rates eased across the curve, giving duration-sensitive assets a macro tailwind.')
    if None not in (sox,ndx):
        if sox>ndx+0.75: transmission_bits.append('Equities did not respond uniformly: semiconductors materially outperformed broad tech.')
        elif sox<ndx-0.75: transmission_bits.append('Semiconductors became the weak link inside technology.')
        else: transmission_bits.append('Equity leadership was relatively broad within technology.')
    if brent is not None and abs(brent)>=1:
        transmission_bits.append(f'Oil moved {pct(brent)}, adding an inflation / growth cross-check.')
    story_transmission={
      'kicker':'TRANSMISSION',
      'title':'How the initial signal spread through the tape',
      'body':' '.join(transmission_bits) if transmission_bits else 'The initial move did not produce a strong secondary confirmation, so the day remained mostly a relative-value and positioning story.',
      'tone':'mixed'
    }

    story_branches=[]
    # Branch 1: equities / breadth
    eq_state='mixed'
    eq_title='Equity branch · breadth versus concentration'
    eq_text=f'S&P {pct(spx)}, Nasdaq {pct(ndx)}, SOX {pct(sox)}'
    if rut is not None: eq_text+=f' and Russell 2000 {pct(rut)}'
    eq_text+='.'
    if None not in (sox,ndx) and sox>ndx+0.75:
        eq_state='supportive'; eq_text+=' Semis led by enough to call the move thematic rather than simply broad beta.'
    elif None not in (sox,ndx) and sox<ndx-0.75:
        eq_state='warning'; eq_text+=' Semis lagged broad tech, weakening the AI leadership signal.'
    if equal and equal.get('1m') is not None and mv('spx','1m') is not None:
        eq_gap=equal.get('1m')-mv('spx','1m')
        eq_text+=f' Equal Weight is {eq_gap:+.2f}% versus the S&P over one month.'
        if eq_gap<-1: eq_text+=' That keeps breadth narrow.'
    story_branches.append({'title':eq_title,'body':eq_text,'tone':eq_state,'tag':'EQUITIES'})

    # Branch 2: rates decomposition
    rate_state='mixed'
    rate_text=f'2Y {num(y2lvl,2)}% ({bps(y2)}), 10Y {num(y10lvl,2)}% ({bps(y10)}), 30Y {num(y30lvl,2)}% ({bps(y30)}).'
    if reallvl is not None: rate_text+=f' 10Y real yield {num(reallvl,2)}%.'
    if belvl is not None: rate_text+=f' Breakeven proxy {num(belvl,2)}%.'
    if tplvl is not None: rate_text+=f' ACM term premium {num(tplvl,2)}% ({bps(tp)}).'
    if y2 is not None and y10 is not None and y2<0<y10:
        rate_state='warning'; rate_text+=' The divergence says the pressure sits further out the curve, not primarily in the next-Fed-move expectation.'
    elif y10 is not None and y10<0:
        rate_state='supportive'; rate_text+=' Falling long yields are a cleaner valuation tailwind.'
    story_branches.append({'title':'Rates branch · what actually repriced','body':rate_text,'tone':rate_state,'tag':'RATES'})

    # Branch 3: credit confirmation — same-day, liquid, public-market proxy
    hyig=(cp.get('hy_vs_ig') or {}).get('1d'); hyig1m=(cp.get('hy_vs_ig') or {}).get('1m')
    hytsy=(cp.get('hy_vs_tsy') or {}).get('1d')
    hyg1d=(cp.get('hy') or {}).get('1d'); lqd1d=(cp.get('ig') or {}).get('1d')
    cp_date=(cp.get('hy') or {}).get('asof') or (cp.get('ig') or {}).get('asof')
    if hyig is not None:
        cr_text=f'As of {cp_date or "latest"}: HYG {pct(hyg1d)} versus LQD {pct(lqd1d)}; HY minus IG relative return {hyig:+.2f}% 1D'
        if hyig1m is not None: cr_text+=f' and {hyig1m:+.2f}% over one month'
        if hytsy is not None: cr_text+=f'. HYG versus intermediate Treasuries was {hytsy:+.2f}% 1D'
        cr_text+='.'
        if hyig<-0.20:
            cr_state='warning'; cr_text+=' High yield is underperforming investment grade enough to count as same-day credit-risk confirmation.'
        elif hyig>0.20:
            cr_state='supportive'; cr_text+=' High yield is outperforming investment grade, so credit beta is not confirming a broad risk-off move.'
        else:
            cr_state='mixed'; cr_text+=' The credit-quality spread is close to neutral, so credit is not a decisive branch today.'
    else:
        cr_state='mixed'; cr_text='Same-day HYG/LQD credit proxy was unavailable; the storyline does not infer a credit signal.'
    story_branches.append({'title':'Credit branch · did risky credit confirm?','body':cr_text,'tone':cr_state,'tag':'CREDIT'})

    # Branch 4: macro cross-check
    macro_state='mixed'
    macro_parts=[]
    if dxy is not None: macro_parts.append(f'DXY {pct(dxy)}')
    if brent is not None: macro_parts.append(f'Brent {pct(brent)}')
    if gold is not None: macro_parts.append(f'gold {pct(gold)}')
    if copper is not None: macro_parts.append(f'copper {pct(copper)}')
    macro_text=', '.join(macro_parts)+'.' if macro_parts else 'Macro cross-check data were limited.'
    if brent is not None and brent>1 and y10 is not None and y10>0:
        macro_state='warning'; macro_text+=' Oil and long yields moving higher together reinforce the inflation / duration branch.'
    elif copper is not None and gold is not None and copper>gold+1:
        macro_state='supportive'; macro_text+=' Copper outperforming gold adds a cyclical-growth confirmation.'
    elif gold is not None and copper is not None and gold>copper+1:
        macro_state='warning'; macro_text+=' Gold outperforming copper leans more defensive.'
    story_branches.append({'title':'Macro branch · inflation, growth or hedge demand?','body':macro_text,'tone':macro_state,'tag':'CROSS-ASSET'})

    if y2 is not None and y10 is not None and y2<0<y10 and None not in (sox,ndx) and sox>ndx+0.75:
        resolution_title='The day landed on a split regime: long-end macro pressure, but concentrated AI strength'
        resolution_body=('The branches do not fully converge into either risk-on or risk-off. Long-duration rates remain a headwind, yet semiconductor leadership shows that the market is still willing to pay for visible AI / compute earnings. '
                         'That makes breadth and same-day HYG-versus-LQD performance the immediate tie-breakers: broader equity participation plus resilient high yield would support expansion of the theme; weaker breadth plus HY underperformance would make concentrated leadership more fragile.')
    elif None not in (hy,ccc) and hy>0 and ccc>hy and spx is not None and spx<0:
        resolution_title='The day finished as a broader risk-off move'
        resolution_body=('Equity weakness was accompanied by worsening lower-quality credit, so the signal moved beyond valuation alone. The market was asking for more compensation for both duration and balance-sheet risk.')
    elif risk>=25:
        resolution_title='The day finished constructive, but confirmation still matters'
        resolution_body=('Risk appetite stayed positive overall. The strongest version of this story would require small caps, equal weight and credit to participate rather than leaving the rally concentrated in a few large themes.')
    elif risk<=-25:
        resolution_title='The day finished defensive'
        resolution_body=('Cross-asset signals leaned toward risk reduction. The key distinction for the next session is whether the stress remains rates-led or broadens further into credit and cyclical growth assets.')
    else:
        resolution_title='The day finished mixed rather than contradictory'
        resolution_body=('Different branches were pricing different things. The correct takeaway is not to force one label, but to identify the dominant macro pressure, the pocket of equity leadership that resisted it, and the markets that did or did not confirm the move.')

    storyline={
      'start':story_start,
      'transmission':story_transmission,
      'branches':story_branches,
      'resolution':{'kicker':'END STATE','title':resolution_title,'body':resolution_body,'tone':'mixed'},
      'next':checks[:4],
      'summary':meeting_summary
    }

    return {'headline':headline,'dek':dek,'evidence':evidence,'meeting_summary':meeting_summary,
            'deep_sections':deep_sections,'stock_notes':stock_notes,'positioning':positioning,
            'checks':checks,'bottom_line':bottom,'storyline':storyline}

# ---------------- PACKS ----------------
def load_symbol_set(symbols):
    out={}
    def one(sym):
        h=yahoo_history(sym); return sym,history_metrics(h,sym)
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs=[ex.submit(one,s) for s in symbols]
        for f in as_completed(futs):
            try:
                s,m=f.result(); out[s]=m
            except: pass
    return out

def market_pack():
    symmap={v[0]:k for k,v in CROSS_ASSET.items()}; data=load_symbol_set(symmap.keys()); out={}
    for sym,m in data.items():
        k=symmap[sym]; m['label']=CROSS_ASSET[k][1]; m['asset_class']=CROSS_ASSET[k][2]; out[k]=m
    return out

def factors_pack(hist):
    rows=[]; sp=hist.get('SPY',{})
    for sym,name in FACTOR_ETFS.items():
        m=hist.get(sym)
        if not m: continue
        rows.append({'symbol':sym,'name':name,'1d':m.get('1d'),'1w':m.get('1w'),'1m':m.get('1m'),'rel1m':(m.get('1m') or 0)-(sp.get('1m') or 0)})
    return sorted(rows,key=lambda x:(x['1d'] if x['1d'] is not None else -999),reverse=True)

def sectors_pack(hist):
    rows=[]; sp=hist.get('SPY',{})
    for sym,name in SECTOR_ETFS.items():
        m=hist.get(sym)
        if not m: continue
        rows.append({'symbol':sym,'name':name,'1d':m.get('1d'),'1w':m.get('1w'),'1m':m.get('1m'),'3m':m.get('3m'),'rel1m':(m.get('1m') or 0)-(sp.get('1m') or 0)})
    return sorted(rows,key=lambda x:(x['1d'] if x['1d'] is not None else -999),reverse=True)

def mover_score(m, relative):
    one=abs(m.get('1d') or 0.0); rel=abs(relative or 0.0); week=min(abs(m.get('1w') or 0.0),8.0)
    return 0.65*one + 0.25*rel + 0.10*week

def stock_monitor_pack():
    core_benches=set(v[2] for v in WATCHLIST.values())
    broad_benches=set(v[2] for v in BROAD_WATCHLIST.values())
    hist=load_symbol_set(set(WATCHLIST)|set(BROAD_WATCHLIST)|core_benches|broad_benches)
    core=[]; material=[]
    for sym,(company,group,bench) in WATCHLIST.items():
        m=hist.get(sym); b=hist.get(bench)
        if not m or m.get('1d') is None: continue
        rel=m['1d']-(b.get('1d') or 0) if b else None
        score=mover_score(m,rel)
        flag=(abs(m['1d'])>=1.5 or abs(rel or 0)>=1.0 or abs(m.get('1w') or 0)>=4.0)
        expl=''; link=''; conf='Monitor'
        if flag:
            news=yahoo_news(sym,company)
            if news:
                category=classify_driver(news[0]['title'])
                if category!='Company / sector news':
                    expl=f'{category}: {news[0]["title"]}'
                    link=news[0]['link']; conf='Headline-linked'
        row={'symbol':sym,'display':sym.replace('.KS',''),'company':company,'group':group,'move':m.get('1d'),'1w':m.get('1w'),'1m':m.get('1m'),'relative':rel,'score':score,'material':flag,'explanation':expl,'link':link,'confidence':conf,'quality_issue':m.get('quality_issue')}
        core.append(row)
        if flag: material.append(row)
    rank={g:i for i,g in enumerate(CORE_GROUP_ORDER)}
    core.sort(key=lambda r:(rank.get(r['group'],999),-abs(r['move'] or 0)))
    material.sort(key=lambda r:-r['score'])
    broad_rows=[]
    for sym,(company,sector,bench) in BROAD_WATCHLIST.items():
        m=hist.get(sym); b=hist.get(bench)
        if not m or m.get('1d') is None: continue
        rel=m['1d']-(b.get('1d') or 0) if b else None
        if abs(m['1d'])<3.0 and abs(rel or 0)<2.0: continue
        broad_rows.append({'symbol':sym,'display':sym,'company':company,'sector':sector,'move':m.get('1d'),'1w':m.get('1w'),'1m':m.get('1m'),'relative':rel,'score':mover_score(m,rel),'quality_issue':m.get('quality_issue')})
    broad_rows.sort(key=lambda r:-r['score']); broad_rows=broad_rows[:10]
    for r in broad_rows:
        r['explanation']=''; r['link']=''; r['confidence']='Monitor'
        news=yahoo_news(r['symbol'],r['company'])
        if news:
            category=classify_driver(news[0]['title'])
            if category!='Company / sector news':
                r['explanation']=f'{category}: {news[0]["title"]}'
                r['link']=news[0]['link']; r['confidence']='Headline-linked'
    return {'core_tape':core,'core_movers':material[:14],'broad_movers':broad_rows}


def treasury_auctions():
    """Upcoming Treasury Note/Bond/TIPS auctions for the rates panel only."""
    out=[]; now=datetime.now(NY_TZ)
    try:
        rows=req('https://www.treasurydirect.gov/TA_WS/securities/upcoming?format=json',timeout=12).json()
    except Exception:
        return out
    for r in rows:
        if r.get('securityType') not in {'Note','Bond','TIPS'}:
            continue
        try:
            dd=datetime.fromisoformat((r.get('auctionDate') or '')[:10]).replace(hour=13,minute=0,tzinfo=NY_TZ)
        except:
            continue
        if dd < now-timedelta(hours=3):
            continue
        term=r.get('securityTerm','')
        amount=fnum(r.get('offeringAmount'))
        s=dd.astimezone(SGT)
        out.append({
            'date':s.strftime('%a, %d %b'),
            'time':s.strftime('%H:%M'),
            'term':term,
            'security_type':r.get('securityType'),
            'amount':amount,
            'long_end':any(x in term for x in ('10-Year','20-Year','30-Year'))
        })
    return sorted(out,key=lambda x:(x['date'],x['time']))

def fed_pack():
    funcs={'curve':treasury_curve,'fred':fred_pack,'acm':acm_term_premium,'cvol':cme_cvol,'fedwatch':fedwatch,'calendar':major_calendar,'auctions':treasury_auctions}
    out={}
    with ThreadPoolExecutor(max_workers=7) as ex:
        futs={ex.submit(fn):k for k,fn in funcs.items()}
        for fut in as_completed(futs):
            k=futs[fut]
            try: out[k]=fut.result()
            except Exception as e: out[k]=[] if k in {'calendar','auctions'} else {'error':str(e)}
    return out

def build_dashboard():
    # Load core public data in parallel
    with ThreadPoolExecutor(max_workers=4) as ex:
        f_market=ex.submit(market_pack); f_fed=ex.submit(fed_pack); f_stocks=ex.submit(stock_monitor_pack)
        tactical_symbols=set(TACTICAL_ASSETS)|set(SECTOR_ETFS)|set(FACTOR_ETFS)|{'ACWI','XLI','XLU'}
        f_hist=ex.submit(load_symbol_set,tactical_symbols)
        market=f_market.result(); fp=f_fed.result(); stocks=f_stocks.result(); histories=f_hist.result()
        movers=stocks.get('core_movers',[]); core_tape=stocks.get('core_tape',[]); broad_movers=stocks.get('broad_movers',[])
    # merge commodity history metrics from market into regime inputs
    fred=fp.get('fred') if isinstance(fp.get('fred'),dict) else {}
    # inflation score includes Brent 1m and, when available, 10Y breakeven change proxy
    regime=build_risk_regime(histories,market,fred)
    curve=fp.get('curve') if isinstance(fp.get('curve'),dict) else {}
    real=fred.get('real10') or {}
    try:
        be=(curve['10Y']['yield']-real['value'])
        prevbe=((curve['10Y']['yield']-curve['10Y']['bp1d']/100)-(real['value']-real['1d']))
        bechg=(be-prevbe)*100
        regime['inflation']=clip(0.65*clip(((market.get('brent') or {}).get('1m') or 0)/12.0)+0.35*clip(bechg/8.0))
        breakeven={'value':be,'bp1d':bechg}
    except: breakeven={'error':'unavailable'}
    signals=tactical_signals(histories,regime)
    sectors=sectors_pack(histories)
    factors=factors_pack(histories)
    lqd=histories.get('LQD') or {}; hyg=histories.get('HYG') or {}; ief=histories.get('IEF') or {}
    def relret(a,b,key):
        av=a.get(key); bv=b.get(key)
        return None if av is None or bv is None else av-bv
    credit_proxy={
      'ig':{'symbol':'LQD','price':lqd.get('price'),'1d':lqd.get('1d'),'1m':lqd.get('1m'),'asof':lqd.get('asof')},
      'hy':{'symbol':'HYG','price':hyg.get('price'),'1d':hyg.get('1d'),'1m':hyg.get('1m'),'asof':hyg.get('asof')},
      'hy_vs_ig':{'1d':relret(hyg,lqd,'1d'),'1m':relret(hyg,lqd,'1m'),'asof':hyg.get('asof') or lqd.get('asof')},
      'hy_vs_tsy':{'1d':relret(hyg,ief,'1d'),'1m':relret(hyg,ief,'1m'),'asof':hyg.get('asof') or ief.get('asof')}
    }
    payload={'market':market,'curve':curve,'fred':fred,'credit_proxy':credit_proxy,'acm':fp.get('acm',{}),'cvol':fp.get('cvol',{}),'fedwatch':fp.get('fedwatch',{}),'calendar':fp.get('calendar',[]),'auctions':fp.get('auctions',[]),
             'movers':movers,'core_tape':core_tape,'broad_movers':broad_movers,'sectors':sectors,'factors':factors,'signals':signals,'regime':regime,'breakeven':breakeven}
    payload['takeaways']=top_takeaways(payload)
    payload['commentary']=build_commentary(payload)
    quality=[]
    for key,row in market.items():
        if row.get('quality_issue'):
            quality.append(f'{row.get("label",key)}: {row["quality_issue"]}')
    for row in core_tape:
        if row.get('quality_issue'):
            quality.append(f'{row.get("display")}: {row["quality_issue"]}')
    payload['quality_issues']=quality
    payload['meta']={
        'updated':sgt_now().isoformat(timespec='seconds'),
        'mode':'scheduled static snapshot',
        'price_policy':'Returns are calculated from consecutive adjusted daily bars; chartPreviousClose is never used.',
        'commentary_policy':'Rule-based commentary only; unsupported catalysts are omitted.'
    }
    return payload


def json_safe(obj):
    if isinstance(obj, dict):
        return {k: json_safe(v) for k,v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj

def main():
    root=Path(__file__).resolve().parents[1]
    target=root/'site'/'data'/'dashboard.json'
    payload=json_safe(build_dashboard())
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'Wrote {target}')
    if payload.get('quality_issues'):
        print('QUALITY WARNINGS:')
        for x in payload['quality_issues']:
            print(' -',x)

if __name__=='__main__':
    main()
